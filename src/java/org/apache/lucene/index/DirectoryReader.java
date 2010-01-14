package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.index.codecs.Codecs;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ReaderUtil;

import org.apache.lucene.search.FieldCache; // not great (circular); used only to purge FieldCache entry on close

/** 
 * An IndexReader which reads indexes with multiple segments.
 */
class DirectoryReader extends IndexReader implements Cloneable {
  protected Directory directory;
  protected boolean readOnly;
  
  protected Codecs codecs;

  IndexWriter writer;

  private IndexDeletionPolicy deletionPolicy;
  private final HashSet<String> synced = new HashSet<String>();
  private Lock writeLock;
  private SegmentInfos segmentInfos;
  private boolean stale;
  private final int termInfosIndexDivisor;

  private boolean rollbackHasChanges;
  private SegmentInfos rollbackSegmentInfos;

  private SegmentReader[] subReaders;
  private int[] starts;                           // 1st docno for each segment
  private Map<String,byte[]> normsCache = new HashMap<String,byte[]>();
  private int maxDoc = 0;
  private int numDocs = -1;
  private boolean hasDeletions = false;

  private MultiFields fields;

//  static IndexReader open(final Directory directory, final IndexDeletionPolicy deletionPolicy, final IndexCommit commit, final boolean readOnly,
//      final int termInfosIndexDivisor) throws CorruptIndexException, IOException {
//    return open(directory, deletionPolicy, commit, readOnly, termInfosIndexDivisor, null);
//  }
  
  static IndexReader open(final Directory directory, final IndexDeletionPolicy deletionPolicy, final IndexCommit commit, final boolean readOnly,
                          final int termInfosIndexDivisor, Codecs codecs) throws CorruptIndexException, IOException {
    final Codecs codecs2;
    if (codecs == null) {
      codecs2 = Codecs.getDefault();
    } else {
      codecs2 = codecs;
    }
    return (IndexReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName, codecs2);
        if (readOnly)
          return new ReadOnlyDirectoryReader(directory, infos, deletionPolicy, termInfosIndexDivisor, codecs2);
        else
          return new DirectoryReader(directory, infos, deletionPolicy, false, termInfosIndexDivisor, codecs2);
      }
    }.run(commit);
  }

  /** Construct reading the named set of readers. */
//  DirectoryReader(Directory directory, SegmentInfos sis, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor) throws IOException {
//    this(directory, sis, deletionPolicy, readOnly, termInfosIndexDivisor, null);
//  }
  
  /** Construct reading the named set of readers. */
  DirectoryReader(Directory directory, SegmentInfos sis, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor, Codecs codecs) throws IOException {
    this.directory = directory;
    this.readOnly = readOnly;
    this.segmentInfos = sis;
    this.deletionPolicy = deletionPolicy;
    this.termInfosIndexDivisor = termInfosIndexDivisor;

    if (codecs == null) {
      this.codecs = Codecs.getDefault();
    } else {
      this.codecs = codecs;
    }
    
    if (!readOnly) {
      // We assume that this segments_N was previously
      // properly sync'd:
      synced.addAll(sis.files(directory, true));
    }

    // To reduce the chance of hitting FileNotFound
    // (and having to retry), we open segments in
    // reverse because IndexWriter merges & deletes
    // the newest segments first.

    SegmentReader[] readers = new SegmentReader[sis.size()];
    for (int i = sis.size()-1; i >= 0; i--) {
      boolean success = false;
      try {
        readers[i] = SegmentReader.get(readOnly, sis.info(i), termInfosIndexDivisor);
        success = true;
      } finally {
        if (!success) {
          // Close all readers we had opened:
          for(i++;i<sis.size();i++) {
            try {
              readers[i].close();
            } catch (Throwable ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    initialize(readers);
  }

  // Used by near real-time search
  DirectoryReader(IndexWriter writer, SegmentInfos infos, int termInfosIndexDivisor, Codecs codecs) throws IOException {
    this.directory = writer.getDirectory();
    this.readOnly = true;
    segmentInfos = infos;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    if (codecs == null) {
      this.codecs = Codecs.getDefault();
    } else {
      this.codecs = codecs;
    }

    
    if (!readOnly) {
      // We assume that this segments_N was previously
      // properly sync'd:
      synced.addAll(infos.files(directory, true));
    }

    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();
    SegmentReader[] readers = new SegmentReader[numSegments];
    final Directory dir = writer.getDirectory();
    int upto = 0;

    for (int i=0;i<numSegments;i++) {
      boolean success = false;
      try {
        final SegmentInfo info = infos.info(upto);
        if (info.dir == dir) {
          readers[upto++] = writer.readerPool.getReadOnlyClone(info, true, termInfosIndexDivisor);
        }
        success = true;
      } finally {
        if (!success) {
          // Close all readers we had opened:
          for(upto--;upto>=0;upto--) {
            try {
              readers[upto].close();
            } catch (Throwable ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    this.writer = writer;

    if (upto < readers.length) {
      // This means some segments were in a foreign Directory
      SegmentReader[] newReaders = new SegmentReader[upto];
      System.arraycopy(readers, 0, newReaders, 0, upto);
      readers = newReaders;
    }

    initialize(readers);
  }

  /** This constructor is only used for {@link #reopen()} */
  DirectoryReader(Directory directory, SegmentInfos infos, SegmentReader[] oldReaders, int[] oldStarts,
                  Map<String,byte[]> oldNormsCache, boolean readOnly, boolean doClone, int termInfosIndexDivisor, Codecs codecs) throws IOException {
    this.directory = directory;
    this.readOnly = readOnly;
    this.segmentInfos = infos;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    if (codecs == null) {
      this.codecs = Codecs.getDefault();
    } else {
      this.codecs = codecs;
    }
    
    if (!readOnly) {
      // We assume that this segments_N was previously
      // properly sync'd:
      synced.addAll(infos.files(directory, true));
    }

    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    Map<String,Integer> segmentReaders = new HashMap<String,Integer>();

    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0; i < oldReaders.length; i++) {
        segmentReaders.put(oldReaders[i].getSegmentName(), Integer.valueOf(i));
      }
    }
    
    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    
    // remember which readers are shared between the old and the re-opened
    // DirectoryReader - we have to incRef those readers
    boolean[] readerShared = new boolean[infos.size()];
    
    for (int i = infos.size() - 1; i>=0; i--) {
      // find SegmentReader for this segment
      Integer oldReaderIndex = segmentReaders.get(infos.info(i).name);
      if (oldReaderIndex == null) {
        // this is a new segment, no old SegmentReader can be reused
        newReaders[i] = null;
      } else {
        // there is an old reader for this segment - we'll try to reopen it
        newReaders[i] = oldReaders[oldReaderIndex.intValue()];
      }

      boolean success = false;
      try {
        SegmentReader newReader;
        if (newReaders[i] == null || infos.info(i).getUseCompoundFile() != newReaders[i].getSegmentInfo().getUseCompoundFile()) {

          // We should never see a totally new segment during cloning
          assert !doClone;

          // this is a new reader; in case we hit an exception we can close it safely
          newReader = SegmentReader.get(readOnly, infos.info(i), termInfosIndexDivisor);
        } else {
          newReader = newReaders[i].reopenSegment(infos.info(i), doClone, readOnly);
        }
        if (newReader == newReaders[i]) {
          // this reader will be shared between the old and the new one,
          // so we must incRef it
          readerShared[i] = true;
          newReader.incRef();
        } else {
          readerShared[i] = false;
          newReaders[i] = newReader;
        }
        success = true;
      } finally {
        if (!success) {
          for (i++; i < infos.size(); i++) {
            if (newReaders[i] != null) {
              try {
                if (!readerShared[i]) {
                  // this is a new subReader that is not used by the old one,
                  // we can close it
                  newReaders[i].close();
                } else {
                  // this subReader is also used by the old reader, so instead
                  // closing we must decRef it
                  newReaders[i].decRef();
                }
              } catch (IOException ignore) {
                // keep going - we want to clean up as much as possible
              }
            }
          }
        }
      }
    }    
    
    // initialize the readers to calculate maxDoc before we try to reuse the old normsCache
    initialize(newReaders);
    
    // try to copy unchanged norms from the old normsCache to the new one
    if (oldNormsCache != null) {
      for (Map.Entry<String,byte[]> entry: oldNormsCache.entrySet()) {
        String field = entry.getKey();
        if (!hasNorms(field)) {
          continue;
        }

        byte[] oldBytes = entry.getValue();

        byte[] bytes = new byte[maxDoc()];

        for (int i = 0; i < subReaders.length; i++) {
          Integer oldReaderIndex = segmentReaders.get(subReaders[i].getSegmentName());

          // this SegmentReader was not re-opened, we can copy all of its norms 
          if (oldReaderIndex != null &&
               (oldReaders[oldReaderIndex.intValue()] == subReaders[i] 
                 || oldReaders[oldReaderIndex.intValue()].norms.get(field) == subReaders[i].norms.get(field))) {
            // we don't have to synchronize here: either this constructor is called from a SegmentReader,
            // in which case no old norms cache is present, or it is called from MultiReader.reopen(),
            // which is synchronized
            System.arraycopy(oldBytes, oldStarts[oldReaderIndex.intValue()], bytes, starts[i], starts[i+1] - starts[i]);
          } else {
            subReaders[i].norms(field, bytes, starts[i]);
          }
        }

        normsCache.put(field, bytes);      // update cache
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    if (hasChanges) {
      buffer.append("*");
    }
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    for(SegmentReader r : subReaders) {
      buffer.append(r);
    }
    buffer.append(')');
    return buffer.toString();
  }

  private void initialize(SegmentReader[] subReaders) {
    this.subReaders = subReaders;
    starts = new int[subReaders.length + 1];    // build starts array
    Bits[] subs = new Bits[subReaders.length];
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();      // compute maxDocs

      if (subReaders[i].hasDeletions()) {
        hasDeletions = true;
      }
      subs[i] = subReaders[i].getDeletedDocs();
    }
    starts[subReaders.length] = maxDoc;

    if (hasDeletions) {
      deletedDocs = new MultiBits(subs, starts);
    } else {
      deletedDocs = null;
    }

    fields = new MultiFields(subReaders, starts);
  }

  private MultiBits deletedDocs;

  // Exposes a slice of an existing Bits as a new Bits.
  // Only used when one provides an external skipDocs (ie,
  // not the del docs from this DirectoryReader), to pull
  // the DocsEnum of the sub readers
  private final static class SubBits implements Bits {
    private final Bits parent;
    private final int start;
    private final int length;

    // start is inclusive; end is exclusive (length = end-start)
    public SubBits(Bits parent, int start, int length) {
      this.parent = parent;
      this.start = start;
      this.length = length;
      assert length >= 0: "length=" + length;
    }
    
    public boolean get(int doc) {
      if (doc >= length) {
        throw new RuntimeException("doc " + doc + " is out of bounds 0 .. " + (length-1));
      }
      assert doc < length: "doc=" + doc + " length=" + length;
      return parent.get(doc+start);
    }
  }
    
  // Concatenates multiple Bits together
  // nocommit -- if none of the subs have deletions we
  // should return null from getDeletedDocs:
  static final class MultiBits implements Bits {
    private final Bits[] subs;
    // this is 1+subs.length, ie the last entry has the maxDoc
    final int[] starts;

    public MultiBits(Bits[] subs, int[] starts) {
      this.subs = subs;
      this.starts = starts;
    }

    public boolean get(int doc) {
      final int reader = ReaderUtil.subIndex(doc, starts);
      final Bits bits = subs[reader];
      if (bits == null) {
        return false;
      } else {
        final int length = starts[1+reader]-starts[reader];
        assert doc - starts[reader] < length: "doc=" + doc + " reader=" + reader + " starts[reader]=" + starts[reader] + " length=" + length;
        return bits.get(doc-starts[reader]);
      }
    }
  }

  @Override
  public Bits getDeletedDocs() {
    return deletedDocs;
  }

  @Override
  public final synchronized Object clone() {
    try {
      return clone(readOnly); // Preserve current readOnly
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public final synchronized IndexReader clone(boolean openReadOnly) throws CorruptIndexException, IOException {
    DirectoryReader newReader = doReopen((SegmentInfos) segmentInfos.clone(), true, openReadOnly);

    if (this != newReader) {
      newReader.deletionPolicy = deletionPolicy;
    }
    newReader.writer = writer;
    // If we're cloning a non-readOnly reader, move the
    // writeLock (if there is one) to the new reader:
    if (!openReadOnly && writeLock != null) {
      // In near real-time search, reader is always readonly
      assert writer == null;
      newReader.writeLock = writeLock;
      newReader.hasChanges = hasChanges;
      newReader.hasDeletions = hasDeletions;
      writeLock = null;
      hasChanges = false;
    }

    return newReader;
  }

  @Override
  public final IndexReader reopen() throws CorruptIndexException, IOException {
    // Preserve current readOnly
    return doReopen(readOnly, null);
  }

  @Override
  public final IndexReader reopen(boolean openReadOnly) throws CorruptIndexException, IOException {
    return doReopen(openReadOnly, null);
  }

  @Override
  public final IndexReader reopen(final IndexCommit commit) throws CorruptIndexException, IOException {
    return doReopen(true, commit);
  }

  private final IndexReader doReopenFromWriter(boolean openReadOnly, IndexCommit commit) throws CorruptIndexException, IOException {
    assert readOnly;

    if (!openReadOnly) {
      throw new IllegalArgumentException("a reader obtained from IndexWriter.getReader() can only be reopened with openReadOnly=true (got false)");
    }

    if (commit != null) {
      throw new IllegalArgumentException("a reader obtained from IndexWriter.getReader() cannot currently accept a commit");
    }

    // TODO: right now we *always* make a new reader; in
    // the future we could have write make some effort to
    // detect that no changes have occurred
    return writer.getReader();
  }

  private IndexReader doReopen(final boolean openReadOnly, IndexCommit commit) throws CorruptIndexException, IOException {
    ensureOpen();

    assert commit == null || openReadOnly;

    // If we were obtained by writer.getReader(), re-ask the
    // writer to get a new reader.
    if (writer != null) {
      return doReopenFromWriter(openReadOnly, commit);
    } else {
      return doReopenNoWriter(openReadOnly, commit);
    }
  }

  private synchronized IndexReader doReopenNoWriter(final boolean openReadOnly, IndexCommit commit) throws CorruptIndexException, IOException {

    if (commit == null) {
      if (hasChanges) {
        // We have changes, which means we are not readOnly:
        assert readOnly == false;
        // and we hold the write lock:
        assert writeLock != null;
        // so no other writer holds the write lock, which
        // means no changes could have been done to the index:
        assert isCurrent();

        if (openReadOnly) {
          return clone(openReadOnly);
        } else {
          return this;
        }
      } else if (isCurrent()) {
        if (openReadOnly != readOnly) {
          // Just fallback to clone
          return clone(openReadOnly);
        } else {
          return this;
        }
      }
    } else {
      if (directory != commit.getDirectory())
        throw new IOException("the specified commit does not match the specified Directory");
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getCurrentSegmentFileName())) {
        if (readOnly != openReadOnly) {
          // Just fallback to clone
          return clone(openReadOnly);
        } else {
          return this;
        }
      }
    }

    return (IndexReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName, codecs);
        return doReopen(infos, false, openReadOnly);
      }
    }.run(commit);
  }

  private synchronized DirectoryReader doReopen(SegmentInfos infos, boolean doClone, boolean openReadOnly) throws CorruptIndexException, IOException {
    DirectoryReader reader;
    if (openReadOnly) {
      reader = new ReadOnlyDirectoryReader(directory, infos, subReaders, starts, normsCache, doClone, termInfosIndexDivisor, null);
    } else {
      reader = new DirectoryReader(directory, infos, subReaders, starts, normsCache, false, doClone, termInfosIndexDivisor, null);
    }
    return reader;
  }

  /** Version number when this IndexReader was opened. */
  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public TermFreqVector[] getTermFreqVectors(int n) throws IOException {
    ensureOpen();
    int i = readerIndex(n);        // find segment num
    return subReaders[i].getTermFreqVectors(n - starts[i]); // dispatch to segment
  }

  @Override
  public TermFreqVector getTermFreqVector(int n, String field)
      throws IOException {
    ensureOpen();
    int i = readerIndex(n);        // find segment num
    return subReaders[i].getTermFreqVector(n - starts[i], field);
  }


  @Override
  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    int i = readerIndex(docNumber);        // find segment num
    subReaders[i].getTermFreqVector(docNumber - starts[i], field, mapper);
  }

  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    int i = readerIndex(docNumber);        // find segment num
    subReaders[i].getTermFreqVector(docNumber - starts[i], mapper);
  }

  /**
   * Checks is the index is optimized (if it has a single segment and no deletions)
   * @return <code>true</code> if the index is optimized; <code>false</code> otherwise
   */
  @Override
  public boolean isOptimized() {
    ensureOpen();
    return segmentInfos.size() == 1 && !hasDeletions();
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)

    // NOTE: multiple threads may wind up init'ing
    // numDocs... but that's harmless
    if (numDocs == -1) {        // check cache
      int n = 0;                // cache miss--recompute
      for (int i = 0; i < subReaders.length; i++)
        n += subReaders[i].numDocs();      // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  // inherit javadoc
  @Override
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    int i = readerIndex(n);                          // find segment num
    return subReaders[i].document(n - starts[i], fieldSelector);    // dispatch to segment reader
  }

  @Override
  public boolean isDeleted(int n) {
    // Don't call ensureOpen() here (it could affect performance)
    final int i = readerIndex(n);                           // find segment num
    return subReaders[i].isDeleted(n - starts[i]);    // dispatch to segment reader
  }

  @Override
  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return hasDeletions;
  }

  @Override
  protected void doDelete(int n) throws CorruptIndexException, IOException {
    numDocs = -1;                             // invalidate cache
    int i = readerIndex(n);                   // find segment num
    subReaders[i].deleteDocument(n - starts[i]);      // dispatch to segment reader
    hasDeletions = true;
  }

  @Override
  protected void doUndeleteAll() throws CorruptIndexException, IOException {
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].undeleteAll();

    hasDeletions = false;
    numDocs = -1;                                 // invalidate cache
  }

  private int readerIndex(int n) {    // find reader for doc n:
    return readerIndex(n, this.starts, this.subReaders.length);
  }
  
  final static int readerIndex(int n, int[] starts, int numSubReaders) {    // find reader for doc n:
    int lo = 0;                                      // search starts array
    int hi = numSubReaders - 1;                  // for first element less

    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = starts[mid];
      if (n < midValue)
        hi = mid - 1;
      else if (n > midValue)
        lo = mid + 1;
      else {                                      // found a match
        while (mid+1 < numSubReaders && starts[mid+1] == midValue) {
          mid++;                                  // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (subReaders[i].hasNorms(field)) return true;
    }
    return false;
  }

  @Override
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    byte[] bytes = normsCache.get(field);
    if (bytes != null)
      return bytes;          // cache hit
    if (!hasNorms(field))
      return null;

    bytes = new byte[maxDoc()];
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].norms(field, bytes, starts[i]);
    normsCache.put(field, bytes);      // update cache
    return bytes;
  }

  @Override
  public synchronized void norms(String field, byte[] result, int offset)
    throws IOException {
    ensureOpen();
    byte[] bytes = normsCache.get(field);
    if (bytes==null && !hasNorms(field)) {
      Arrays.fill(result, offset, result.length, Similarity.getDefault().encodeNormValue(1.0f));
    } else if (bytes != null) {                           // cache hit
      System.arraycopy(bytes, 0, result, offset, maxDoc());
    } else {
      for (int i = 0; i < subReaders.length; i++) {      // read from segments
        subReaders[i].norms(field, result, offset + starts[i]);
      }
    }
  }

  @Override
  protected void doSetNorm(int n, String field, byte value)
    throws CorruptIndexException, IOException {
    synchronized (normsCache) {
      normsCache.remove(field);                         // clear cache      
    }
    int i = readerIndex(n);                           // find segment num
    subReaders[i].setNorm(n-starts[i], field, value); // dispatch
  }

  @Override
  public TermEnum terms() throws IOException {
    ensureOpen();
    //nocommit: investigate this opto
    if (subReaders.length == 1) {
      // Optimize single segment case:
      return subReaders[0].terms();
    } else {
     return new MultiTermEnum(this, subReaders, starts, null);
    }
  }

  @Override
  public TermEnum terms(Term term) throws IOException {
    ensureOpen();
    if (subReaders.length == 1) {
      // Optimize single segment case:
      return subReaders[0].terms(term);
    } else {
      return new MultiTermEnum(this, subReaders, starts, term);
    }
  }

  @Override
  public int docFreq(Term t) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++)
      total += subReaders[i].docFreq(t);
    return total;
  }

  @Override
  public int docFreq(String field, TermRef term) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++) {
      total += subReaders[i].docFreq(field, term);
    }
    return total;
  }

  @Override
  public TermDocs termDocs() throws IOException {
    ensureOpen();
    if (subReaders.length == 1) {
      // Optimize single segment case:
      return subReaders[0].termDocs();
    } else {
      return new MultiTermDocs(this, subReaders, starts);
    }
  }

  @Override
  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    if (subReaders.length == 1) {
      // Optimize single segment case:
      return subReaders[0].termDocs(term);
    } else {
      return super.termDocs(term);
    }
  }

  @Override
  public Fields fields() throws IOException {
    if (subReaders.length == 1) {
      // Optimize the single reader case
      return subReaders[0].fields();
    } else {
      return fields;
    }
  }

  @Override
  public TermPositions termPositions() throws IOException {
    ensureOpen();
    if (subReaders.length == 1) {
      // Optimize single segment case:
      return subReaders[0].termPositions();
    } else {
      return new MultiTermPositions(this, subReaders, starts);
    }
  }

  /**
   * Tries to acquire the WriteLock on this directory. this method is only valid if this IndexReader is directory
   * owner.
   *
   * @throws StaleReaderException  if the index has changed since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws org.apache.lucene.store.LockObtainFailedException
   *                               if another writer has this index open (<code>write.lock</code> could not be
   *                               obtained)
   * @throws IOException           if there is a low-level IO error
   */
  @Override
  protected void acquireWriteLock() throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {

    if (readOnly) {
      // NOTE: we should not reach this code w/ the core
      // IndexReader classes; however, an external subclass
      // of IndexReader could reach this.
      ReadOnlySegmentReader.noWrite();
    }

    if (segmentInfos != null) {
      ensureOpen();
      if (stale)
        throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");

      if (writeLock == null) {
        Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
        if (!writeLock.obtain(IndexWriter.WRITE_LOCK_TIMEOUT)) // obtain write lock
          throw new LockObtainFailedException("Index locked for write: " + writeLock);
        this.writeLock = writeLock;

        // we have to check whether index has changed since this reader was opened.
        // if so, this reader is no longer valid for deletion
        if (SegmentInfos.readCurrentVersion(directory, codecs) > segmentInfos.getVersion()) {
          stale = true;
          this.writeLock.release();
          this.writeLock = null;
          throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
        }
      }
    }
  }

  /**
   * Commit changes resulting from delete, undeleteAll, or setNorm operations
   * <p/>
   * If an exception is hit, then either no changes or all changes will have been committed to the index (transactional
   * semantics).
   *
   * @throws IOException if there is a low-level IO error
   */
  @Override
  protected void doCommit(Map<String,String> commitUserData) throws IOException {
    if (hasChanges) {
      segmentInfos.setUserData(commitUserData);
      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      IndexFileDeleter deleter = new IndexFileDeleter(directory,
                                                      deletionPolicy == null ? new KeepOnlyLastCommitDeletionPolicy() : deletionPolicy,
                                                      segmentInfos, null, null, codecs);

      // Checkpoint the state we are about to change, in
      // case we have to roll back:
      startCommit();

      boolean success = false;
      try {
        for (int i = 0; i < subReaders.length; i++)
          subReaders[i].commit();

        // Sync all files we just wrote
        final Collection<String> files = segmentInfos.files(directory, false);
        for (final String fileName : files) { 
          if (!synced.contains(fileName)) {
            assert directory.fileExists(fileName);
            directory.sync(fileName);
            synced.add(fileName);
          }
        }

        segmentInfos.commit(directory);
        success = true;
      } finally {

        if (!success) {

          // Rollback changes that were made to
          // SegmentInfos but failed to get [fully]
          // committed.  This way this reader instance
          // remains consistent (matched to what's
          // actually in the index):
          rollbackCommit();

          // Recompute deletable files & remove them (so
          // partially written .del files, etc, are
          // removed):
          deleter.refresh();
        }
      }

      // Have the deleter remove any now unreferenced
      // files due to this commit:
      deleter.checkpoint(segmentInfos, true);
      deleter.close();

      if (writeLock != null) {
        writeLock.release();  // release write lock
        writeLock = null;
      }
    }
    hasChanges = false;
  }

  void startCommit() {
    rollbackHasChanges = hasChanges;
    rollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();
    for (int i = 0; i < subReaders.length; i++) {
      subReaders[i].startCommit();
    }
  }

  void rollbackCommit() {
    hasChanges = rollbackHasChanges;
    for (int i = 0; i < segmentInfos.size(); i++) {
      // Rollback each segmentInfo.  Because the
      // SegmentReader holds a reference to the
      // SegmentInfo we can't [easily] just replace
      // segmentInfos, so we reset it in place instead:
      segmentInfos.info(i).reset(rollbackSegmentInfos.info(i));
    }
    rollbackSegmentInfos = null;
    for (int i = 0; i < subReaders.length; i++) {
      subReaders[i].rollbackCommit();
    }
  }

  @Override
  public Map<String,String> getCommitUserData() {
    ensureOpen();
    return segmentInfos.getUserData();
  }

  /**
   * Check whether this IndexReader is still using the current (i.e., most recently committed) version of the index.  If
   * a writer has committed any changes to the index since this reader was opened, this will return <code>false</code>,
   * in which case you must open a new IndexReader in order
   * to see the changes.  Use {@link IndexWriter#commit} to
   * commit changes to the index.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException           if there is a low-level IO error
   */
  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    return SegmentInfos.readCurrentVersion(directory, codecs) == segmentInfos.getVersion();
  }

  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    normsCache = null;
    for (int i = 0; i < subReaders.length; i++) {
      // try to close each reader, even if an exception is thrown
      try {
        subReaders[i].decRef();
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }

    // NOTE: only needed in case someone had asked for
    // FieldCache for top-level reader (which is generally
    // not a good idea):
    FieldCache.DEFAULT.purge(this);

    // throw the first exception
    if (ioe != null) throw ioe;
  }

  @Override
  public Collection<String> getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    return getFieldNames(fieldNames, this.subReaders);
  }
  
  static Collection<String> getFieldNames (IndexReader.FieldOption fieldNames, IndexReader[] subReaders) {
    // maintain a unique set of field names
    Set<String> fieldSet = new HashSet<String>();
    for (IndexReader reader : subReaders) {
      Collection<String> names = reader.getFieldNames(fieldNames);
      fieldSet.addAll(names);
    }
    return fieldSet;
  } 
  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    return subReaders;
  }

  /** Returns the directory this index resides in. */
  @Override
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return directory;
  }

  @Override
  public int getTermInfosIndexDivisor() {
    return termInfosIndexDivisor;
  }

  /**
   * Expert: return the IndexCommit that this reader has opened.
   * <p/>
   * <p><b>WARNING</b>: this API is new and experimental and may suddenly change.</p>
   */
  @Override
  public IndexCommit getIndexCommit() throws IOException {
    return new ReaderCommit(segmentInfos, directory);
  }

  /** @see org.apache.lucene.index.IndexReader#listCommits */
  public static Collection<IndexCommit> listCommits(Directory dir) throws IOException {
    return listCommits(dir, Codecs.getDefault());
  }

  /** @see org.apache.lucene.index.IndexReader#listCommits */
  public static Collection<IndexCommit> listCommits(Directory dir, Codecs codecs) throws IOException {
    final String[] files = dir.listAll();

    Collection<IndexCommit> commits = new ArrayList<IndexCommit>();

    SegmentInfos latest = new SegmentInfos();
    latest.read(dir, codecs);
    final long currentGen = latest.getGeneration();

    commits.add(new ReaderCommit(latest, dir));

    for(int i=0;i<files.length;i++) {

      final String fileName = files[i];

      if (fileName.startsWith(IndexFileNames.SEGMENTS) &&
          !fileName.equals(IndexFileNames.SEGMENTS_GEN) &&
          SegmentInfos.generationFromSegmentsFileName(fileName) < currentGen) {

        SegmentInfos sis = new SegmentInfos();
        try {
          // IOException allowed to throw there, in case
          // segments_N is corrupt
          sis.read(dir, fileName, codecs);
        } catch (FileNotFoundException fnfe) {
          // LUCENE-948: on NFS (and maybe others), if
          // you have writers switching back and forth
          // between machines, it's very likely that the
          // dir listing will be stale and will claim a
          // file segments_X exists when in fact it
          // doesn't.  So, we catch this and handle it
          // as if the file does not exist
          sis = null;
        }

        if (sis != null)
          commits.add(new ReaderCommit(sis, dir));
      }
    }

    return commits;
  }

  private static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    long version;
    final boolean isOptimized;
    final Map<String,String> userData;

    ReaderCommit(SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getCurrentSegmentFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(dir, true));
      version = infos.getVersion();
      generation = infos.getGeneration();
      isOptimized = infos.size() == 1 && !infos.info(0).hasDeletions();
    }

    @Override
    public boolean isOptimized() {
      return isOptimized;
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return dir;
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }
  }
  
  private final static class TermsWithBase {
    Terms terms;
    int base;
    int length;
    Bits skipDocs;

    public TermsWithBase(IndexReader reader, int base, String field) throws IOException {
      this.base = base;
      length = reader.maxDoc();
      assert length >= 0: "length=" + length;
      skipDocs = reader.getDeletedDocs();
      terms = reader.fields().terms(field);
    }
  }

  private final static class FieldsEnumWithBase {
    FieldsEnum fields;
    String current;
    int base;
    int length;
    Bits skipDocs;

    public FieldsEnumWithBase(IndexReader reader, int base) throws IOException {
      this.base = base;
      length = reader.maxDoc();
      assert length >= 0: "length=" + length;
      skipDocs = reader.getDeletedDocs(); 
      fields = reader.fields().iterator();
    }
  }

  private final static class TermsEnumWithBase {
    final TermsEnum terms;
    final int base;
    final int length;
    TermRef current;
    final Bits skipDocs;

    public TermsEnumWithBase(FieldsEnumWithBase start, TermsEnum terms, TermRef term) {
      this.terms = terms;
      current = term;
      skipDocs = start.skipDocs;
      base = start.base;
      length = start.length;
      assert length >= 0: "length=" + length;
    }

    public TermsEnumWithBase(TermsWithBase start, TermsEnum terms, TermRef term) {
      this.terms = terms;
      current = term;
      skipDocs = start.skipDocs;
      base = start.base;
      length = start.length;
      assert length >= 0: "length=" + length;
    }
  }

  private final static class DocsEnumWithBase {
    DocsEnum docs;
    int base;
  }

  private final static class FieldMergeQueue extends PriorityQueue<FieldsEnumWithBase> {
    FieldMergeQueue(int size) {
      initialize(size);
    }

    @Override
    protected final boolean lessThan(FieldsEnumWithBase fieldsA, FieldsEnumWithBase fieldsB) {
      return fieldsA.current.compareTo(fieldsB.current) < 0;
    }
  }

  private final static class TermMergeQueue extends PriorityQueue<TermsEnumWithBase> {
    TermRef.Comparator termComp;
    TermMergeQueue(int size) {
      initialize(size);
    }

    @Override
    protected final boolean lessThan(TermsEnumWithBase termsA, TermsEnumWithBase termsB) {
      final int cmp = termComp.compare(termsA.current, termsB.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return termsA.base < termsB.base;
      }
    }
  }

  // Exposes flex API, merged from flex API of
  // sub-segments.
  final static class MultiFields extends Fields {
    private final IndexReader[] readers;
    private final int[] starts;
    private final HashMap<String,MultiTerms> terms = new HashMap<String,MultiTerms>();

    public MultiFields(IndexReader[] readers, int[] starts) {
      this.readers = readers;
      this.starts = starts;
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      FieldsEnumWithBase[] subs = new FieldsEnumWithBase[readers.length];
      for(int i=0;i<subs.length;i++) {
        subs[i] = new FieldsEnumWithBase(readers[i], starts[i]);
      }
      return new MultiFieldsEnum(subs);
    }

    @Override
    public Terms terms(String field) throws IOException {
      MultiTerms result = terms.get(field);
      if (result == null) {

        // First time this field is requested, we create & add to terms:
        List<TermsWithBase> subs = new ArrayList<TermsWithBase>();

        // Gather all sub-readers that share this field
        for(int i=0;i<readers.length;i++) {
          Terms subTerms = readers[i].fields().terms(field);
          if (subTerms != null) {
            subs.add(new TermsWithBase(readers[i], starts[i], field));
          }
        }
        result = new MultiTerms(subs.toArray(new TermsWithBase[]{}));
        terms.put(field, result);
      }
      return result;
    }
  }

  // Exposes flex API, merged from flex API of
  // sub-segments.
  private final static class MultiTerms extends Terms {
    private final TermsWithBase[] subs;
    private final TermRef.Comparator termComp;

    public MultiTerms(TermsWithBase[] subs) throws IOException {
      this.subs = subs;
      
      TermRef.Comparator _termComp = null;
      for(int i=0;i<subs.length;i++) {
        if (_termComp == null) {
          _termComp = subs[i].terms.getTermComparator();
        } else {
          // We cannot merge sub-readers that have
          // different TermComps
          final TermRef.Comparator subTermComp = subs[i].terms.getTermComparator();
          if (subTermComp != null && !subTermComp.equals(_termComp)) {
            throw new IllegalStateException("sub-readers have different TermRef.Comparators; cannot merge");
          }
        }
      }
      termComp = _termComp;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new MultiTermsEnum(subs.length).reset(subs);
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      return termComp;
    }
  }

  // Exposes flex API, merged from flex API of
  // sub-segments.  This does a merge sort, by field name,
  // of the sub-readers.
  private final static class MultiFieldsEnum extends FieldsEnum {
    private final FieldMergeQueue queue;

    // Holds sub-readers containing field we are currently
    // on, popped from queue.
    private final FieldsEnumWithBase[] top;
    private int numTop;

    // Re-used TermsEnum
    private final MultiTermsEnum terms;

    private String currentField;
    
    MultiFieldsEnum(FieldsEnumWithBase[] subs) throws IOException {
      terms = new MultiTermsEnum(subs.length);
      queue = new FieldMergeQueue(subs.length);
      top = new FieldsEnumWithBase[subs.length];

      // Init q
      for(int i=0;i<subs.length;i++) {
        subs[i].current = subs[i].fields.next();
        if (subs[i].current != null) {
          queue.add(subs[i]);
        }
      }
    }

    public String field() {
      assert currentField != null;
      assert numTop > 0;
      return currentField;
    }

    @Override
    public String next() throws IOException {

      // restore queue
      for(int i=0;i<numTop;i++) {
        top[i].current = top[i].fields.next();
        if (top[i].current != null) {
          queue.add(top[i]);
        } else {
          // no more fields in this sub-reader
        }
      }

      numTop = 0;

      // gather equal top fields
      if (queue.size() > 0) {
        while(true) {
          top[numTop++] = queue.pop();
          if (queue.size() == 0 || (queue.top()).current != top[0].current) {
            break;
          }
        }
        currentField = top[0].current;
      } else {
        currentField = null;
      }

      return currentField;
    }

    @Override
    public TermsEnum terms() throws IOException {
      return terms.reset(top, numTop);
    }
  }

  // Exposes flex API, merged from flex API of
  // sub-segments.  This does a merge sort, by term text, of
  // the sub-readers.
  private static final class MultiTermsEnum extends TermsEnum {
    
    private final TermMergeQueue queue;
    private final TermsEnumWithBase[] subs;
    private final TermsEnumWithBase[] top;
    int numTop;
    int numSubs;
    private TermRef current;
    private final MultiDocsEnum docs;
    private TermRef.Comparator termComp;

    MultiTermsEnum(int size) {
      queue = new TermMergeQueue(size);
      top = new TermsEnumWithBase[size];
      subs = new TermsEnumWithBase[size];
      docs = new MultiDocsEnum(size);
    }

    @Override
    public TermRef term() {
      return current;
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      return termComp;
    }

    MultiTermsEnum reset(TermsWithBase[] terms) throws IOException {
      assert terms.length <= top.length;
      numSubs = 0;
      numTop = 0;
      termComp = null;
      queue.clear();
      for(int i=0;i<terms.length;i++) {
        final TermsEnum termsEnum = terms[i].terms.iterator();
        if (termsEnum != null) {
          if (termComp == null) {
            queue.termComp = termComp = termsEnum.getTermComparator();
          } else {
            // We cannot merge sub-readers that have
            // different TermComps
            final TermRef.Comparator subTermComp = termsEnum.getTermComparator();
            if (subTermComp != null && !subTermComp.equals(termComp)) {
              throw new IllegalStateException("sub-readers have different TermRef.Comparators; cannot merge");
            }
          }
          final TermRef term = termsEnum.next();
          if (term != null) {
            subs[numSubs] = new TermsEnumWithBase(terms[i], termsEnum, term);
            queue.add(subs[numSubs]);
            numSubs++;
          } else {
            // field has no terms
          }
        }
      }

      return this;
    }

    MultiTermsEnum reset(FieldsEnumWithBase[] fields, int numFields) throws IOException {
      assert numFields <= top.length;
      numSubs = 0;
      numTop = 0;
      termComp = null;
      queue.clear();
      for(int i=0;i<numFields;i++) {
        final TermsEnum terms = fields[i].fields.terms();
        if (terms != null) {
          final TermRef term = terms.next();
          if (term != null) {
            if (termComp == null) {
              queue.termComp = termComp = terms.getTermComparator();
            } else {
              assert termComp.equals(terms.getTermComparator());
            }
            subs[numSubs] = new TermsEnumWithBase(fields[i], terms, term);
            queue.add(subs[numSubs]);
            numSubs++;
          } else {
            // field has no terms
          }
        }
      }

      return this;
    }

    @Override
    public SeekStatus seek(TermRef term) throws IOException {
      queue.clear();
      numTop = 0;
      for(int i=0;i<numSubs;i++) {
        final SeekStatus status = subs[i].terms.seek(term);
        if (status == SeekStatus.FOUND) {
          top[numTop++] = subs[i];
          subs[i].current = term;
        } else if (status == SeekStatus.NOT_FOUND) {
          subs[i].current = subs[i].terms.term();
          assert subs[i].current != null;
          queue.add(subs[i]);
        } else {
          // enum exhausted
        }
      }

      if (numTop > 0) {
        current = term;
        return SeekStatus.FOUND;
      } else if (queue.size() > 0) {
        pullTop();
        return SeekStatus.NOT_FOUND;
      } else {
        return SeekStatus.END;
      }
    }

    @Override
    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    private final void pullTop() {
      // extract all subs from the queue that have the same
      // top term
      assert numTop == 0;
      while(true) {
        top[numTop++] = queue.pop();
        if (queue.size() == 0 || !(queue.top()).current.termEquals(top[0].current)) {
          break;
        }
      } 
      current = top[0].current;
    }

    private final void pushTop() throws IOException {
      // call next() on each top, and put back into queue
      for(int i=0;i<numTop;i++) {
        top[i].current = top[i].terms.next();
        if (top[i].current != null) {
          queue.add(top[i]);
        } else {
          // no more fields in this reader
        }
      }
      numTop = 0;
    }

    @Override
    public TermRef next() throws IOException {
      // restore queue
      pushTop();

      // gather equal top fields
      if (queue.size() > 0) {
        pullTop();
      } else {
        current = null;
      }

      return current;
    }

    @Override
    public int docFreq() {
      int sum = 0;
      for(int i=0;i<numTop;i++) {
        sum += top[i].terms.docFreq();
      }
      return sum;
    }

    @Override
    public DocsEnum docs(Bits skipDocs) throws IOException {
      return docs.reset(top, numTop, skipDocs);
    }
  }

  private static final class MultiDocsEnum extends DocsEnum {
    final DocsEnumWithBase[] subs;
    int numSubs;
    int upto;
    DocsEnum currentDocs;
    int currentBase;
    Bits skipDocs;
    int doc = -1;

    MultiDocsEnum(int count) {
      subs = new DocsEnumWithBase[count];
    }

    MultiDocsEnum reset(TermsEnumWithBase[] subs, final int numSubs, final Bits skipDocs) throws IOException {
      this.numSubs = 0;
      this.skipDocs = skipDocs;
      for(int i=0;i<numSubs;i++) {
        Bits bits = null;
        boolean handled = false;

        assert subs[i].length >= 0: "subs[" + i + " of " + numSubs + "].length=" + subs[i].length;

        // Optimize for common case: requested skip docs is
        // simply our (DiretoryReader's) deleted docs.  In
        // this case, we just pull the skipDocs from the sub
        // reader, rather than making the inefficient
        // Sub(Multi(sub-readers)):
        if (skipDocs instanceof MultiBits) {
          MultiBits multiBits = (MultiBits) skipDocs;
          int reader = ReaderUtil.subIndex(subs[i].base, multiBits.starts);
          assert reader < multiBits.starts.length-1: " reader=" + reader + " multiBits.starts.length=" + multiBits.starts.length;
          final int length = multiBits.starts[reader+1] - multiBits.starts[reader];
          if (multiBits.starts[reader] == subs[i].base &&
              length == subs[i].length) {
            bits = multiBits.subs[reader];
            handled = true;
          }
        }

        if (!handled && skipDocs != null) {
          // custom case: requested skip docs is foreign
          bits = new SubBits(skipDocs, subs[i].base, subs[i].length);
        }

        final DocsEnum docs = subs[i].terms.docs(bits);
        if (docs != null) {
          this.subs[this.numSubs] = new DocsEnumWithBase();
          this.subs[this.numSubs].docs = docs;
          this.subs[this.numSubs].base = subs[i].base;
          this.numSubs++;
        }
      }
      upto = -1;
      currentDocs = null;
      return this;
    }

    @Override
    public int freq() {
      return currentDocs.freq();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int read(final int docs[], final int freqs[]) throws IOException {
      while (true) {
        while (currentDocs == null) {
          if (upto == numSubs-1) {
            return 0;
          } else {
            upto++;
            currentDocs = subs[upto].docs;
            currentBase = subs[upto].base;
          }
        }
        final int end = currentDocs.read(docs, freqs);
        if (end == 0) {          // none left in segment
          currentDocs = null;
        } else {            // got some
          for (int i = 0; i < end; i++) {
            docs[i] += currentBase;
          }
          return end;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      while(true) {
        if (currentDocs != null) {
          final int doc = currentDocs.advance(target-currentBase);
          if (doc == NO_MORE_DOCS) {
            currentDocs = null;
          } else {
            return this.doc = doc + currentBase;
          }
        } else if (upto == numSubs-1) {
          return this.doc = NO_MORE_DOCS;
        } else {
          upto++;
          currentDocs = subs[upto].docs;
          currentBase = subs[upto].base;
        }
      }
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (currentDocs == null) {
          if (upto == numSubs-1) {
            return this.doc = NO_MORE_DOCS;
          } else {
            upto++;
            currentDocs = subs[upto].docs;
            currentBase = subs[upto].base;
          }
        }

        final int doc = currentDocs.nextDoc();
        if (doc != NO_MORE_DOCS) {
          return this.doc = currentBase + doc;
        } else {
          currentDocs = null;
        }
      }
    }

    @Override
    public PositionsEnum positions() throws IOException {
      return currentDocs.positions();
    }
  }

  // @deprecated This is pre-flex API
  // Exposes pre-flex API by doing on-the-fly merging
  // pre-flex API to each segment
  static class MultiTermEnum extends TermEnum {
    IndexReader topReader; // used for matching TermEnum to TermDocs
    private LegacySegmentMergeQueue queue;
  
    private Term term;
    private int docFreq;
    final LegacySegmentMergeInfo[] matchingSegments; // null terminated array of matching segments

    public MultiTermEnum(IndexReader topReader, IndexReader[] readers, int[] starts, Term t)
      throws IOException {
      this.topReader = topReader;
      queue = new LegacySegmentMergeQueue(readers.length);
      matchingSegments = new LegacySegmentMergeInfo[readers.length+1];
      for (int i = 0; i < readers.length; i++) {
        IndexReader reader = readers[i];
        TermEnum termEnum;
  
        if (t != null) {
          termEnum = reader.terms(t);
        } else {
          termEnum = reader.terms();
        }
  
        LegacySegmentMergeInfo smi = new LegacySegmentMergeInfo(starts[i], termEnum, reader);
        smi.ord = i;
        if (t == null ? smi.next() : termEnum.term() != null)
          queue.add(smi);          // initialize queue
        else
          smi.close();
      }
  
      if (t != null && queue.size() > 0) {
        next();
      }
    }
  
    @Override
    public boolean next() throws IOException {
      for (int i=0; i<matchingSegments.length; i++) {
        LegacySegmentMergeInfo smi = matchingSegments[i];
        if (smi==null) break;
        if (smi.next())
          queue.add(smi);
        else
          smi.close(); // done with segment
      }
      
      int numMatchingSegments = 0;
      matchingSegments[0] = null;

      LegacySegmentMergeInfo top = queue.top();

      if (top == null) {
        term = null;
        return false;
      }
  
      term = top.term;
      docFreq = 0;
  
      while (top != null && term.compareTo(top.term) == 0) {
        matchingSegments[numMatchingSegments++] = top;
        queue.pop();
        docFreq += top.termEnum.docFreq();    // increment freq
        top = queue.top();
      }

      matchingSegments[numMatchingSegments] = null;
      return true;
    }
  
    @Override
    public Term term() {
      return term;
    }
  
    @Override
    public int docFreq() {
      return docFreq;
    }
  
    @Override
    public void close() throws IOException {
      queue.close();
    }
  }

  // @deprecated This is pre-flex API
  // Exposes pre-flex API by doing on-the-fly merging
  // pre-flex API to each segment
  static class MultiTermDocs implements TermDocs {
    IndexReader topReader;  // used for matching TermEnum to TermDocs
    protected IndexReader[] readers;
    protected int[] starts;
    protected Term term;
  
    protected int base = 0;
    protected int pointer = 0;
  
    private TermDocs[] readerTermDocs;
    protected TermDocs current;              // == readerTermDocs[pointer]

    private MultiTermEnum tenum;  // the term enum used for seeking... can be null
    int matchingSegmentPos;  // position into the matching segments from tenum
    LegacySegmentMergeInfo smi;     // current segment mere info... can be null

    public MultiTermDocs(IndexReader topReader, IndexReader[] r, int[] s) {
      this.topReader = topReader;
      readers = r;
      starts = s;
  
      readerTermDocs = new TermDocs[r.length];
    }

    public int doc() {
      return base + current.doc();
    }
    public int freq() {
      return current.freq();
    }
  
    public void seek(Term term) {
      this.term = term;
      this.base = 0;
      this.pointer = 0;
      this.current = null;
      this.tenum = null;
      this.smi = null;
      this.matchingSegmentPos = 0;
    }
  
    public void seek(TermEnum termEnum) throws IOException {
      seek(termEnum.term());
      if (termEnum instanceof MultiTermEnum) {
        tenum = (MultiTermEnum)termEnum;
        if (topReader != tenum.topReader)
          tenum = null;
      }
    }
  
    public boolean next() throws IOException {
      for(;;) {
        if (current!=null && current.next()) {
          return true;
        }
        else if (pointer < readers.length) {
          if (tenum != null) {
            smi = tenum.matchingSegments[matchingSegmentPos++];
            if (smi==null) {
              pointer = readers.length;
              return false;
            }
            pointer = smi.ord;
          }
          base = starts[pointer];
          current = termDocs(pointer++);
        } else {
          return false;
        }
      }
    }
  
    /** Optimized implementation. */
    public int read(final int[] docs, final int[] freqs) throws IOException {
      while (true) {
        while (current == null) {
          if (pointer < readers.length) {      // try next segment
            if (tenum != null) {
              smi = tenum.matchingSegments[matchingSegmentPos++];
              if (smi==null) {
                pointer = readers.length;
                return 0;
              }
              pointer = smi.ord;
            }
            base = starts[pointer];
            current = termDocs(pointer++);
          } else {
            return 0;
          }
        }
        int end = current.read(docs, freqs);
        if (end == 0) {          // none left in segment
          current = null;
        } else {            // got some
          final int b = base;        // adjust doc numbers
          for (int i = 0; i < end; i++)
           docs[i] += b;
          return end;
        }
      }
    }
  
   /* A Possible future optimization could skip entire segments */ 
    public boolean skipTo(int target) throws IOException {
      for(;;) {
        if (current != null && current.skipTo(target-base)) {
          return true;
        } else if (pointer < readers.length) {
          if (tenum != null) {
            LegacySegmentMergeInfo smi = tenum.matchingSegments[matchingSegmentPos++];
            if (smi==null) {
              pointer = readers.length;
              return false;
            }
            pointer = smi.ord;
          }
          base = starts[pointer];
          current = termDocs(pointer++);
        } else
          return false;
      }
    }
  
    private TermDocs termDocs(int i) throws IOException {
      TermDocs result = readerTermDocs[i];
      if (result == null)
        result = readerTermDocs[i] = termDocs(readers[i]);
      if (smi != null) {
        assert(smi.ord == i);
        assert(smi.termEnum.term().equals(term));
        result.seek(smi.termEnum);
      } else {
        result.seek(term);
      }
      return result;
    }
  
    protected TermDocs termDocs(IndexReader reader)
      throws IOException {
      return term==null ? reader.termDocs(null) : reader.termDocs();
    }
  
    public void close() throws IOException {
      for (int i = 0; i < readerTermDocs.length; i++) {
        if (readerTermDocs[i] != null)
          readerTermDocs[i].close();
      }
    }
  }

  // @deprecated This is pre-flex API
  // Exposes pre-flex API by doing on-the-fly merging
  // pre-flex API to each segment
  static class MultiTermPositions extends MultiTermDocs implements TermPositions {
    public MultiTermPositions(IndexReader topReader, IndexReader[] r, int[] s) {
      super(topReader,r,s);
    }
  
    @Override
    protected TermDocs termDocs(IndexReader reader) throws IOException {
      return reader.termPositions();
    }
  
    public int nextPosition() throws IOException {
      return ((TermPositions)current).nextPosition();
    }
    
    public int getPayloadLength() {
      return ((TermPositions)current).getPayloadLength();
    }
     
    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return ((TermPositions)current).getPayload(data, offset);
    }
  
  
    // TODO: Remove warning after API has been finalized
    public boolean isPayloadAvailable() {
      return ((TermPositions) current).isPayloadAvailable();
    }
  }
}
