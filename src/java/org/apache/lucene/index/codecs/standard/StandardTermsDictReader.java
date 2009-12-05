package org.apache.lucene.index.codecs.standard;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.pulsing.PulsingDocsWriter.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.cache.Cache;
import org.apache.lucene.util.cache.DoubleBarrelLRUCache;

/** Handles a terms dict, but defers all details of postings
 *  reading to an instance of {@TermsDictDocsReader}. This
 *  terms dict codec is meant to be shared between
 *  different postings codecs, but, it's certainly possible
 *  to make a codec that has its own terms dict writer/reader. */

public class StandardTermsDictReader extends FieldsProducer {
  private final IndexInput in;

  private final StandardDocsProducer docs;

  final TreeMap<String,FieldReader> fields = new TreeMap<String,FieldReader>();

  private final String segment;
  private StandardTermsIndexReader indexReader;

  private final TermRef.Comparator termComp;
  
  public StandardTermsDictReader(StandardTermsIndexReader indexReader, Directory dir, FieldInfos fieldInfos, String segment, StandardDocsProducer docs, int readBufferSize,
                                 TermRef.Comparator termComp)
    throws IOException {
    
    this.segment = segment;
    this.docs = docs;

    this.termComp = termComp;
    
    String file = IndexFileNames.segmentFileName(segment, StandardCodec.TERMS_EXTENSION);
    //nocommit
    if(!dir.fileExists(file)) {
      in = null;
      return;
    }
    in = dir.openInput(file, readBufferSize);


    boolean success = false;
    try {
      Codec.checkHeader(in, StandardTermsDictWriter.CODEC_NAME, StandardTermsDictWriter.VERSION_CURRENT);

      final long dirOffset = in.readLong();


      // Have DocsProducer init itself
      docs.start(in);

      // Read per-field details
      in.seek(dirOffset);

      final int numFields = in.readInt();

      // mxx
      if (Codec.DEBUG) {
        System.out.println(Thread.currentThread().getName() + ": stdr create seg=" + segment + " numFields=" + numFields + " hasProx?=" + fieldInfos.hasProx());
      }

      for(int i=0;i<numFields;i++) {
        final int field = in.readInt();
        final long numTerms = in.readLong();
        final long termsStartPointer = in.readLong();
        final StandardTermsIndexReader.FieldReader fieldIndexReader;
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        if (Codec.DEBUG) {
          System.out.println("  stdr: load field=" + fieldInfo.name + " numTerms=" + numTerms);
        }
        if (indexReader != null) {
          fieldIndexReader = indexReader.getField(fieldInfo);
        } else {
          fieldIndexReader = null;
        }
        if (numTerms > 0) {
          assert !fields.containsKey(fieldInfo.name);
          fields.put(fieldInfo.name, new FieldReader(fieldIndexReader, fieldInfo, numTerms, termsStartPointer));
        }
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }

    this.indexReader = indexReader;
  }

  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    // nocommit -- must handle case where segment has become
    // a CFS since we originall opened; maybe Directory
    // should be passed in?
    indexReader.loadTermsIndex(indexDivisor);
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if(indexReader != null) {
          indexReader.close();
        }
      } finally {
        if(in != null) {
          in.close();
        }
      }
    } finally {
      try {
        if(docs != null) {
          docs.close();
        }
      } finally {
        for(FieldReader field : fields.values()) {
          field.close();
        }
      }
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.TERMS_EXTENSION));
  }

  public static void getExtensions(Collection<String> extensions) {
    extensions.add(StandardCodec.TERMS_EXTENSION);
  }

  @Override
  public FieldsEnum iterator() {
    return new TermFieldsEnum();
  }

  @Override
  public Terms terms(String field) throws IOException {
    if (Codec.DEBUG) {
      System.out.println("stdr.terms field=" + field + " found=" + fields.get(field));
    }
    return fields.get(field);
  }

  // Iterates through all known fields
  private class TermFieldsEnum extends FieldsEnum {
    final Iterator<FieldReader> it;
    FieldReader current;

    TermFieldsEnum() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (Codec.DEBUG) {
        System.out.println("stdr.tfe.next seg=" + segment);
        //new Throwable().printStackTrace(System.out);
      }
      if (it.hasNext()) {
        current = it.next();
        if (Codec.DEBUG) {
          System.out.println("  hasNext set current field=" + current.fieldInfo.name);
        }
        return current.fieldInfo.name;
      } else {
        current = null;
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      return current.iterator();
    }
  }
  
  private class FieldReader extends Terms {
    private final CloseableThreadLocal<ThreadResources> threadResources = new CloseableThreadLocal<ThreadResources>();
    final long numTerms;
    final FieldInfo fieldInfo;
    final long termsStartPointer;
    final StandardTermsIndexReader.FieldReader indexReader;
    private final static int DEFAULT_CACHE_SIZE = 1024;
    // Used for caching the least recently looked-up Terms
    private final Cache<TermRef,CacheEntry> termsCache = new DoubleBarrelLRUCache<TermRef,CacheEntry>(DEFAULT_CACHE_SIZE);

    FieldReader(StandardTermsIndexReader.FieldReader fieldIndexReader, FieldInfo fieldInfo, long numTerms, long termsStartPointer) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.termsStartPointer = termsStartPointer;
      this.indexReader = fieldIndexReader;
    }

    @Override
    public int docFreq(TermRef text) throws IOException {
      ThreadResources resources = getThreadResources();
      if (resources.termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
        return resources.termsEnum.docFreq();
      } else {
        return 0;
      }
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      return termComp;
    }

    // nocommit -- figure out how to do this one: we want to
    // reuse the thread private TermsEnum, but, get a
    // clone'd docs, somehow.  This way if code is using the
    // API sequentially, we match performance of current
    // trunk (though, really, such code ought to get their
    // own terms enum and use its seek...)
    /*
    @Override
    public DocsEnum docs(Bits skipDocs, TermRef text) throws IOException {
      ThreadResources resources = getThreadResources();
      if (resources.termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
        return resources.termsEnum.docs(skipDocs);
      } else {
        return null;
      }
    }
    */
    
    public void close() {
      threadResources.close();
    }
    
    protected ThreadResources getThreadResources() throws IOException {
      ThreadResources resources = (ThreadResources) threadResources.get();
      if (resources == null) {
        // Cache does not have to be thread-safe, it is only used by one thread at the same time
        resources = new ThreadResources(new SegmentTermsEnum());
        threadResources.set(resources);
      }
      return resources;
    }
    
    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public long getUniqueTermCount() {
      return numTerms;
    }

    // Iterates through terms in this field
    private class SegmentTermsEnum extends TermsEnum {
      private final IndexInput in;
      private final DeltaBytesReader bytesReader;
      // nocommit: long?
      private int termUpto;
      private final StandardDocsProducer.Reader docs;
      private int docFreq;
      private final StandardTermsIndexReader.TermsIndexResult indexResult = new StandardTermsIndexReader.TermsIndexResult();
      
      SegmentTermsEnum() throws IOException {
        if (Codec.DEBUG) {
          System.out.println("tdr " + this + ": CREATE TermsEnum field=" + fieldInfo.name + " startPos=" + termsStartPointer + " seg=" + segment);
        }
        in = (IndexInput) StandardTermsDictReader.this.in.clone();
        in.seek(termsStartPointer);
        bytesReader = new DeltaBytesReader(in);
        if (Codec.DEBUG) {
          System.out.println("  bytesReader=" + bytesReader);
        }
        docs = StandardTermsDictReader.this.docs.reader(fieldInfo, in);
      }

      @Override
      public TermRef.Comparator getTermComparator() {
        return termComp;
      }

      /** Seeks until the first term that's >= the provided
       *  text; returns SeekStatus.FOUND if the exact term
       *  is found, SeekStatus.NOT_FOUND if a different term
       *  was found, SeekStatus.END if we hit EOF */
      @Override
      public SeekStatus seek(TermRef term) throws IOException {

        CacheEntry entry = null;
        TermRef entryKey = null;

        if (docs.canCaptureState()) {
          entry = termsCache.get(term);
          if (entry != null) {
            docFreq = entry.freq;
            bytesReader.term.copy(term);
            docs.setState(entry, docFreq);
            termUpto = entry.termUpTo;
            // nocommit -- would be better to do this lazy?
            in.seek(entry.filePointer);
            return SeekStatus.FOUND;
          }
        }
        
        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ":stdr.seek(text=" + fieldInfo.name + ":" + term + ") seg=" + segment);
        }

        // nocommit -- test if this is really
        // helping/necessary -- that compareTerm isn't that
        // cheap, and, how often do callers really seek to
        // the term they are already on (it's silly to do
        // so) -- I'd prefer such silly apps take the hit,
        // not well behaved apps?

        if (bytesReader.started && termUpto < numTerms && bytesReader.term.termEquals(term)) {
          // nocommit -- not right if text is ""?
          // mxx
          if (Codec.DEBUG) {
            System.out.println(Thread.currentThread().getName() + ":  already here!");
          }
          // nocommit -- cache this
          return SeekStatus.FOUND;
        }

        // nocommit -- carry over logic from TermInfosReader,
        // here, that avoids the binary search if the seek
        // is w/in the current index block

        // nocommit -- also, not sure it'll help, but, we
        // can bound this binary search, since we know the
        // term ord we are now on, and we can compare how
        // this new term compars to our current term

        // Find latest index term that's <= our text:
        indexReader.getIndexOffset(term, indexResult);

        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ":  index pos=" + indexResult.position + " termFP=" + indexResult.offset + " term=" + indexResult.term + " this=" + this);
        }

        in.seek(indexResult.offset);

        // NOTE: the first next() after an index seek is
        // wasteful, since it redundantly reads the same
        // bytes into the buffer
        bytesReader.reset(indexResult.term);

        termUpto = indexResult.position;
        assert termUpto>=0: "termUpto=" + termUpto;

        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ":  set termUpto=" + termUpto);
        }

        // Now, scan:

        //int indexCount = 0;
        //int lastIndexCount = 0;
        //int scanCnt = 0;
        while(next() != null) {
          //scanCnt++;
          final int cmp = termComp.compare(bytesReader.term, term);
          if (cmp == 0) {
            // mxx
            if (Codec.DEBUG) {
              System.out.println(Thread.currentThread().getName() + ":  seek done found term=" + bytesReader.term);
              //new Throwable().printStackTrace(System.out);
            }

            // nocommit -- see how often an already
            // NOT_FOUND is then sent back here?  silly for
            // apps to do so... but we should see if Lucene
            // does 
            if (docs.canCaptureState()) {
              // Store in cache
              entry = docs.captureState();
              entryKey = (TermRef) bytesReader.term.clone();
              entry.freq = docFreq;
              entry.termUpTo = termUpto;
              entry.filePointer = in.getFilePointer();
            
              termsCache.put(entryKey, entry);
            }
            return SeekStatus.FOUND;
          } else if (cmp > 0) {
            // mxx
            if (Codec.DEBUG) {
              System.out.println(Thread.currentThread().getName() + ":  seek done did not find term=" + term + " found instead: " + bytesReader.term);
            }
            return SeekStatus.NOT_FOUND;
          }

          // We should not cross another indexed term while
          // scanning:

          // nocommit -- not correct that we call
          // isIndexTerm, twice
          //indexCount += indexReader.isIndexTerm(termUpto, docFreq) ? 1:0;
          //assert lastIndexCount < indexDivisor: " indexCount=" + lastIndexCount + " indexDivisor=" + indexDivisor;
          //lastIndexCount = indexCount;

          // mxx
          //System.out.println(Thread.currentThread().getName() + ":  cycle");
        }

        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ": seek done did not find term=" + term + ": hit EOF");
        }
        return SeekStatus.END;
      }

      @Override
      public SeekStatus seek(long pos) throws IOException {
        if (pos >= numTerms) {
          return SeekStatus.END;
        }
        indexReader.getIndexOffset(pos, indexResult);
        in.seek(indexResult.offset);

        // NOTE: the first next() after an index seek is
        // wasteful, since it redundantly reads the same
        // bytes into the buffer
        bytesReader.reset(indexResult.term);

        termUpto = indexResult.position;
        assert termUpto>=0: "termUpto=" + termUpto;

        // Now, scan:
        int left = (int) (1 + pos - termUpto);
        while(left > 0) {
          TermRef term = next();
          assert term != null;
          left--;
        }

        // always found
        return SeekStatus.FOUND;
      }

      @Override
      public TermRef term() {
        return bytesReader.term;
      }

      @Override
      public long ord() {
        return termUpto;
      }

      @Override
      public TermRef next() throws IOException {
        if (termUpto >= numTerms) {
          return null;
        }
        if (Codec.DEBUG) {
          System.out.println("tdr.next: field=" + fieldInfo.name + " termsInPointer=" + in.getFilePointer() + " vs len=" + in.length() + " seg=" + segment);
          //new Throwable().printStackTrace(System.out);
        }
        bytesReader.read();
        docFreq = in.readVInt();
        if (Codec.DEBUG) {
          System.out.println("  text=" + bytesReader.term + " freq=" + docFreq);
        }
        // TODO: would be cleaner, but space-wasting, to
        // simply record a bit into each index entry as to
        // whether it's an index entry or not... or,
        // possibly store a "how many terms until next index
        // entry" in each index entry, but that'd require
        // some tricky lookahead work when writing the index
        final boolean isIndex = indexReader.isIndexTerm(termUpto, docFreq);

        // mxx
        // System.out.println(Thread.currentThread().getName() + ": isIndex=" + isIndex);

        docs.readTerm(docFreq, isIndex);
        termUpto++;
        if (Codec.DEBUG) {
          System.out.println("  termUpto=" + termUpto + " vs numTerms=" + numTerms + " fp=" + in.getFilePointer());
        }
        return bytesReader.term;
      }

      @Override
      public int docFreq() {
        return docFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs) throws IOException {
        // nocommit
        if (Codec.DEBUG) {
          System.out.println("stdr.docs");
        }
        DocsEnum docsEnum = docs.docs(skipDocs);
        if (Codec.DEBUG) {
          docsEnum.desc = fieldInfo.name + ":" + bytesReader.term;
        }
        return docsEnum;
      }
    }
  }

  // nocommit -- scrutinize API
  public static class CacheEntry {
    int termUpTo;                                 // ord for this term
    long filePointer;                             // fp into the terms dict primary file (_X.tis)

    // nocommit -- belongs in Pulsing's CacheEntry class:
    public int freq;
    public Document docs[];
    public boolean pendingIndexTerm;
  }
  
  /**
   * Per-thread resources managed by ThreadLocal
   */
  private static final class ThreadResources {
    final TermsEnum termsEnum;

    ThreadResources(TermsEnum termsEnum) {
      this.termsEnum = termsEnum;
    }
  }
}
