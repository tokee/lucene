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
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.cache.Cache;
import org.apache.lucene.util.cache.DoubleBarrelLRUCache;
import org.apache.lucene.util.BytesRef;

/** Handles a terms dict, but decouples all details of
 *  doc/freqs/positions reading to an instance of {@link
 *  StandardPostingsReader}.  This class is reusable for
 *  codecs that use a different format for
 *  docs/freqs/positions (though codecs are also free to
 *  make their own terms dict impl).
 *
 * <p>This class also interacts with an instance of {@link
 * StandardTermsIndexReader}, to abstract away the specific
 * implementation of the terms dict index. */

public class StandardTermsDictReader extends FieldsProducer {
  // Open input to the main terms dict file (_X.tis)
  private final IndexInput in;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  private final StandardPostingsReader postingsReader;

  private final TreeMap<String,FieldReader> fields = new TreeMap<String,FieldReader>();

  private final String segment;

  // Comparator that orders our terms
  private final BytesRef.Comparator termComp;

  // Caches the most recently looked-up Terms:
  private final Cache<FieldAndTerm,TermState> termsCache;

  // Reads the terms index
  private StandardTermsIndexReader indexReader;

  // Used as key for the terms cache
  private static class FieldAndTerm {
    String field;
    BytesRef term;

    public FieldAndTerm() {
    }

    public FieldAndTerm(FieldAndTerm other) {
      field = other.field;
      term = new BytesRef(other.term);
    }

    public boolean equals(Object _other) {
      FieldAndTerm other = (FieldAndTerm) _other;
      return other.field == field && term.bytesEquals(other.term);
    }

    public int hashCode() {
      return field.hashCode() * 31 + term.hashCode();
    }
  }
  
  public StandardTermsDictReader(StandardTermsIndexReader indexReader, Directory dir, FieldInfos fieldInfos, String segment, StandardPostingsReader postingsReader, int readBufferSize,
                                 BytesRef.Comparator termComp, int termsCacheSize)
    throws IOException {
    
    this.segment = segment;
    this.postingsReader = postingsReader;
    termsCache = new DoubleBarrelLRUCache<FieldAndTerm,TermState>(termsCacheSize);

    this.termComp = termComp;
    
    in = dir.openInput(IndexFileNames.segmentFileName(segment, StandardCodec.TERMS_EXTENSION),
                       readBufferSize);

    boolean success = false;
    try {
      Codec.checkHeader(in, StandardTermsDictWriter.CODEC_NAME, StandardTermsDictWriter.VERSION_CURRENT);

      final long dirOffset = in.readLong();

      // Have PostingsReader init itself
      postingsReader.init(in);

      // Read per-field details
      in.seek(dirOffset);

      final int numFields = in.readInt();

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
        fieldIndexReader = indexReader.getField(fieldInfo);
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
    indexReader.loadTermsIndex(indexDivisor);
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if (indexReader != null) {
          indexReader.close();
        }
      } finally {
        // null so if an app hangs on to us (ie, we are not
        // GCable, despite being closed) we still free most
        // ram
        indexReader = null;
        if (in != null) {
          in.close();
        }
      }
    } finally {
      try {
        if (postingsReader != null) {
          postingsReader.close();
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

  // Iterates through all fields
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

  private class FieldReader extends Terms implements Closeable {
    final long numTerms;
    final FieldInfo fieldInfo;
    final long termsStartPointer;
    final StandardTermsIndexReader.FieldReader indexReader;

    FieldReader(StandardTermsIndexReader.FieldReader fieldIndexReader, FieldInfo fieldInfo, long numTerms, long termsStartPointer) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.termsStartPointer = termsStartPointer;
      this.indexReader = fieldIndexReader;
    }

    @Override
    public BytesRef.Comparator getComparator() {
      return termComp;
    }

    public void close() {
      super.close();
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
      private final TermState state;
      private boolean seekPending;
      private final StandardTermsIndexReader.TermsIndexResult indexResult = new StandardTermsIndexReader.TermsIndexResult();
      private final FieldAndTerm fieldTerm = new FieldAndTerm();

      SegmentTermsEnum() throws IOException {
        if (Codec.DEBUG) {
          System.out.println("tdr " + this + ": CREATE TermsEnum field=" + fieldInfo.name + " startPos=" + termsStartPointer + " seg=" + segment);
        }
        in = (IndexInput) StandardTermsDictReader.this.in.clone();
        in.seek(termsStartPointer);
        bytesReader = new DeltaBytesReader(in);
        fieldTerm.field = fieldInfo.name;
        state = postingsReader.newTermState();
        state.ord = -1;
      }

      @Override
      public BytesRef.Comparator getComparator() {
        return termComp;
      }

      /** Seeks until the first term that's >= the provided
       *  text; returns SeekStatus.FOUND if the exact term
       *  is found, SeekStatus.NOT_FOUND if a different term
       *  was found, SeekStatus.END if we hit EOF */
      @Override
      public SeekStatus seek(BytesRef term) throws IOException {

        if (Codec.DEBUG) {
          Codec.debug("stdr.seek(text=" + fieldInfo.name + ":" + term + ") seg=" + segment);
          new Throwable().printStackTrace(System.out);
        }

        // Check cache
        fieldTerm.term = term;
        TermState cachedState = termsCache.get(fieldTerm);
        
        if (cachedState != null) {

          state.copy(cachedState);

          seekPending = true;
          bytesReader.term.copy(term);

          if (Codec.DEBUG) {
            Codec.debug("  cache hit!  term=" + bytesReader.term + " " + cachedState);
          }

          return SeekStatus.FOUND;
        }

        if (Codec.DEBUG) {
          Codec.debug("  cache miss!");
        }

        boolean doSeek = true;

        if (state.ord != -1) {
          // we are positioned

          final int cmp = termComp.compare(bytesReader.term, term);

          if (cmp == 0) {
            // already at the requested term
            if (Codec.DEBUG) {
              Codec.debug("  already here!");
            }

            return SeekStatus.FOUND;
          }

          if (cmp < 0 &&
              indexReader.nextIndexTerm(state.ord, indexResult) &&
              termComp.compare(indexResult.term, term) > 0) {
            // Optimization: requested term is within the
            // same index block we are now in; skip seeking
            // (but do scanning):
            doSeek = false;
          }
        }

        // Useed only for assert:
        final int startOrd;

        if (doSeek) {

          // As index to find biggest index term that's <=
          // our text:
          indexReader.getIndexOffset(term, indexResult);

          if (Codec.DEBUG) {
            Codec.debug(" index pos=" + indexResult.position + " termFP=" + indexResult.offset + " term=" + indexResult.term + " this=" + this);
          }

          in.seek(indexResult.offset);
          seekPending = false;

          // NOTE: the first next() after an index seek is
          // wasteful, since it redundantly reads the same
          // bytes into the buffer.  We could avoid storing
          // those bytes in the primary file, but then when
          // scanning over an index term we'd have to
          // special case it:
          bytesReader.reset(indexResult.term);
          
          state.ord = (int) indexResult.position-1;
          assert state.ord >= -1: "ord=" + state.ord;

          startOrd = (int) indexResult.position;

          if (Codec.DEBUG) {
            Codec.debug("  set ord=" + state.ord);
          }
        } else {
          startOrd = -1;
          if (Codec.DEBUG) {
            Codec.debug(": use scanning only (no seek)");
          }
        }

        // Now scan:
        while(next() != null) {
          final int cmp = termComp.compare(bytesReader.term, term);
          if (cmp == 0) {
            if (Codec.DEBUG) {
              Codec.debug("  seek done found term=" + bytesReader.term);
            }

            if (doSeek) {
              // Store in cache
              FieldAndTerm entryKey = new FieldAndTerm(fieldTerm);
              cachedState = (TermState) state.clone();
              // this is fp after current term
              cachedState.filePointer = in.getFilePointer();
              termsCache.put(entryKey, cachedState);
              if (Codec.DEBUG) {
                Codec.debug("  save to cache  term=" + fieldTerm.term + " " + cachedState);
              }
            }
            if (Codec.DEBUG) {
              Codec.debug("  found term=" + fieldTerm.term);
            }
              
            return SeekStatus.FOUND;
          } else if (cmp > 0) {
            if (Codec.DEBUG) {
              Codec.debug("  seek done did not find term=" + term + " found instead: " + bytesReader.term);
            }
            return SeekStatus.NOT_FOUND;
          }

          // The purpose of the terms dict index is to seek
          // the enum to the closest index term before the
          // term we are looking for.  So, we should never
          // cross another index term (besides the first
          // one) while we are scanning:
          assert state.ord == startOrd || !indexReader.isIndexTerm(state.ord, state.docFreq);
        }

        if (Codec.DEBUG) {
          Codec.debug("  seek done did not find term=" + term + ": hit EOF");
        }

        return SeekStatus.END;
      }

      @Override
      public SeekStatus seek(long ord) throws IOException {

        // TODO: should we cache term lookup by ord as well...?

        if (ord >= numTerms) {
          state.ord = numTerms-1;
          return SeekStatus.END;
        }

        indexReader.getIndexOffset(ord, indexResult);
        in.seek(indexResult.offset);
        seekPending = false;

        // NOTE: the first next() after an index seek is
        // wasteful, since it redundantly reads the same
        // bytes into the buffer
        bytesReader.reset(indexResult.term);

        state.ord = indexResult.position-1;
        assert state.ord >= -1: "ord=" + state.ord;

        // Now, scan:
        int left = (int) (ord - state.ord);
        while(left > 0) {
          final BytesRef term = next();
          assert term != null;
          left--;
        }

        // always found
        return SeekStatus.FOUND;
      }

      @Override
      public BytesRef term() {
        return bytesReader.term;
      }

      @Override
      public long ord() {
        return state.ord;
      }

      @Override
      public BytesRef next() throws IOException {

        if (Codec.DEBUG) {
          Codec.debug("tdr.next: field=" + fieldInfo.name + " tis.fp=" + in.getFilePointer() + " vs len=" + in.length() + " seg=" + segment);
        }

        if (seekPending) {
          if (Codec.DEBUG) {
            Codec.debug("  do pending seek " + state);
          }
          seekPending = false;
          in.seek(state.filePointer);
        }
        
        if (state.ord >= numTerms-1) {
          return null;
        }

        bytesReader.read();
        state.docFreq = in.readVInt();

        if (Codec.DEBUG) {
          Codec.debug("  text=" + bytesReader.term + " freq=" + state.docFreq + " tis=" + in);
        }

        // TODO: would be cleaner, but space-wasting, to
        // simply record a bit into each index entry as to
        // whether it's an index entry or not, rather than
        // re-compute that information... or, possibly store
        // a "how many terms until next index entry" in each
        // index entry, but that'd require some tricky
        // lookahead work when writing the index

        // nocommit -- this call to isIndexTerm is not
        // right, when indexDivisor > 1?  ie, this will
        // return false for entries that actually are index
        // terms, and then the postings impl will read the
        // wrong offset.  make a test...
        postingsReader.readTerm(in,
                                fieldInfo, state,
                                indexReader.isIndexTerm(1+state.ord, state.docFreq));

        state.ord++;

        if (Codec.DEBUG) {
          Codec.debug("  ord=" + state.ord + " vs numTerms=" + numTerms + " fp=" + in.getFilePointer());
        }

        return bytesReader.term;
      }

      @Override
      public int docFreq() {
        return state.docFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("stdr.docs");
        }
        DocsEnum docsEnum = postingsReader.docs(fieldInfo, state, skipDocs, reuse);
        if (Codec.DEBUG) {
          docsEnum.desc = fieldInfo.name + ":" + bytesReader.term.toString();
        }
        return docsEnum;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("stdr.docsAndPositions omitTF=" + fieldInfo.omitTermFreqAndPositions);
        }
        if (fieldInfo.omitTermFreqAndPositions) {
          return null;
        } else {
          DocsAndPositionsEnum postingsEnum = postingsReader.docsAndPositions(fieldInfo, state, skipDocs, reuse);
          if (Codec.DEBUG) {
            if (postingsEnum != null) {
              postingsEnum.desc = fieldInfo.name + ":" + bytesReader.term.toString();
            }
          }
          if (Codec.DEBUG) {
            Codec.debug("  return enum=" + postingsEnum);
          }
          return postingsEnum;
        }
      }
    }
  }
}
