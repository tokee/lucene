package org.apache.lucene.index.codecs.pulsing;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.standard.StandardDocsProducer;
import org.apache.lucene.index.codecs.pulsing.PulsingDocsWriter.Document;
import org.apache.lucene.index.codecs.standard.StandardTermsDictReader.CacheEntry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Concrete class that reads the current doc/freq/skip
 *  postings format */

// nocommit -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class PulsingDocsReader extends StandardDocsProducer {

  // Fallback reader for non-pulsed terms:
  final StandardDocsProducer wrappedDocsReader;
  IndexInput termsIn;
  int maxPulsingDocFreq;

  public PulsingDocsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize, StandardDocsProducer wrappedDocsReader) throws IOException {
    this.wrappedDocsReader = wrappedDocsReader;
  }

  @Override
  public void start(IndexInput termsIn) throws IOException {
    this.termsIn = termsIn;
    Codec.checkHeader(termsIn, PulsingDocsWriter.CODEC, PulsingDocsWriter.VERSION_START);
    maxPulsingDocFreq = termsIn.readVInt();
    wrappedDocsReader.start(termsIn);
  }

  @Override
  public Reader reader(FieldInfo fieldInfo, IndexInput termsIn) throws IOException {
    return new PulsingReader(fieldInfo, termsIn, wrappedDocsReader.reader(fieldInfo, termsIn));
  }

  class PulsingReader extends Reader {

    final IndexInput termsIn;
    final FieldInfo fieldInfo;
    final boolean omitTF;
    final boolean storePayloads;
    int docFreq;

    // Holds pulsed docs
    final Document[] docs;

    private boolean pendingIndexTerm;
    private final Reader wrappedReader;

    PulsingReader(FieldInfo fieldInfo, IndexInput termsIn, Reader wrappedReader) {
      this.termsIn = termsIn;                     // not cloned
      this.fieldInfo = fieldInfo;
      this.wrappedReader = wrappedReader;
      omitTF = fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;
      docs = new Document[maxPulsingDocFreq];
      for(int i=0;i<maxPulsingDocFreq;i++) {
        docs[i] = new Document();
      }
    }

    @Override
    public void readTerm(int docFreq, boolean isIndexTerm) throws IOException {

      if (Codec.DEBUG) {
        System.out.println("pulsr.readTerm docFreq=" + docFreq + " indexTerm=" + isIndexTerm);
      }

      this.docFreq = docFreq;

      pendingIndexTerm |= isIndexTerm;

      if (docFreq <= maxPulsingDocFreq) {

        if (Codec.DEBUG) {
          System.out.println("  pulsed");
        }

        // Inlined into terms dict -- read everything in

        // TODO: maybe only read everything in lazily?  But
        // then we'd need to store length so we could seek
        // over it when docs/pos enum was not requested

        // TODO: it'd be better to share this encoding logic
        // in some inner codec that knows how to write a
        // single doc / single position, etc.  This way if a
        // given codec wants to store other interesting
        // stuff, it could use this pulsing code to do so
        int docID = 0;
        for(int i=0;i<docFreq;i++) {
          final Document doc = docs[i];
          final int code = termsIn.readVInt();
          if (omitTF) {
            docID += code;
            doc.numPositions = 1;
            if (Codec.DEBUG) {
              System.out.println("  doc=" + docID);
            }
          } else {
            docID += code>>>1;
            if ((code & 1) != 0) {
              doc.numPositions = 1;
            } else {
              doc.numPositions = termsIn.readVInt();
            }
            
            if (Codec.DEBUG) {
              System.out.println("  doc=" + docID + " numPos=" + doc.numPositions);
            }

            if (doc.numPositions > doc.positions.length) {
              doc.reallocPositions(doc.numPositions);
            }

            int position = 0;
            int payloadLength = -1;

            for(int j=0;j<doc.numPositions;j++) {
              final PulsingDocsWriter.Position pos = doc.positions[j];
              final int code2 = termsIn.readVInt();
              if (storePayloads) {
                position += code2 >>> 1;
                if ((code2 & 1) != 0) {
                  payloadLength = termsIn.readVInt();
                }

                if (payloadLength > 0) {
                  if (pos.payload == null) {
                    pos.payload = new BytesRef();
                    pos.payload.bytes = new byte[payloadLength];
                  } else if (payloadLength > pos.payload.bytes.length) {
                    pos.payload.grow(payloadLength);
                  }
                  pos.payload.length = payloadLength;
                  termsIn.readBytes(pos.payload.bytes, 0, payloadLength);
                } else if (pos.payload != null) {
                  pos.payload.length = 0;
                }
              } else {
                position += code2;
              }
              pos.pos = position;
            }
          }
          doc.docID = docID;
        }
        
      } else {
        if (Codec.DEBUG) {
          System.out.println("  not pulsed pass isIndex=" + pendingIndexTerm);
        }
        wrappedReader.readTerm(docFreq, pendingIndexTerm);
        pendingIndexTerm = false;
      }
    }

    final PulsingDocsEnum docsEnum = new PulsingDocsEnum();

    @Override
    public DocsEnum docs(Bits skipDocs) throws IOException {
      if (docFreq <= maxPulsingDocFreq) {
        docsEnum.reset(skipDocs);
        return docsEnum;
      } else {
        return wrappedReader.docs(skipDocs);
      }
    }

    class PulsingDocsEnum extends DocsEnum {
      private int nextRead;
      private Bits skipDocs;
      private Document doc;

      public void close() {}

      void reset(Bits skipDocs) {
        this.skipDocs = skipDocs;
        nextRead = 0;
      }

      @Override
      public int nextDoc() {
        while(true) {
          if (nextRead >= docFreq) {
            return NO_MORE_DOCS;
          } else {
            doc = docs[nextRead++];
            if (skipDocs == null || !skipDocs.get(doc.docID)) {
              return doc.docID;
            }
          }
        }
      }

      @Override
      public int read(int[] retDocs, int[] retFreqs) {
        int i=0;
        // nocommit -- ob1?
        while(nextRead < docFreq) {
          doc = docs[nextRead++];
          if (skipDocs == null || !skipDocs.get(doc.docID)) {
            retDocs[i] = doc.docID;
            if (omitTF)
              retFreqs[i] = 0;
            else
              retFreqs[i] = doc.numPositions;
            i++;
          }
        }
        return i;
      }

      public int ord() {
        assert nextRead <= docFreq;
        return nextRead-1;
      }

      @Override
      public int freq() {
        return doc.numPositions;
      }

      @Override
      public int docID() {
        return doc.docID;
      }

      class PulsingPositionsEnum extends PositionsEnum {
        int nextRead;
        PulsingDocsWriter.Position pos;

        // nocommit -- this is only here to emulate how
        // other codecs disallow retrieving the payload more
        // than once
        private boolean payloadRetrieved;

        void reset() {
          nextRead = 0;
          payloadRetrieved = false;
        }

        @Override
        public int next() {
          assert nextRead < doc.numPositions;
          pos = doc.positions[nextRead++];
          payloadRetrieved = false;
          return pos.pos;
        }

        @Override
        public int getPayloadLength() {
          return pos.payload == null ? 0 : pos.payload.length;
        }

        @Override
        public boolean hasPayload() {
          return pos.payload != null && pos.payload.length > 0;
        }

        @Override
        public BytesRef getPayload() {
          return pos.payload;
        }
      }
      
      final PulsingPositionsEnum positions = new PulsingPositionsEnum();

      @Override
      public PositionsEnum positions() throws IOException {
        positions.reset();
        return positions;
      }

      @Override
      public int advance(int target) throws IOException {
        int doc;
        while((doc=nextDoc()) != NO_MORE_DOCS) {
          if (doc >= target)
            return doc;
        }
        return NO_MORE_DOCS;
      }
    }

    @Override
    public CacheEntry captureState() throws IOException {
      CacheEntry cacheEntry = wrappedReader.captureState();
      cacheEntry.docs = new Document[docs.length];
      for(int i = 0; i < docs.length; i++) {
        cacheEntry.docs[i] = (Document) docs[i].clone();
      }
      cacheEntry.pendingIndexTerm = pendingIndexTerm;

      return cacheEntry;
    }

    @Override
    public void setState(CacheEntry state, int docFreq) throws IOException {
      this.docFreq = docFreq;
      pendingIndexTerm = state.pendingIndexTerm;
      wrappedReader.setState(state, docFreq);
      for(int i = 0; i < docs.length; i++) {
        docs[i] = (Document) state.docs[i].clone();
      }
    }
    
    @Override
    public boolean canCaptureState() {
      return true;
    }
  }

  @Override
  public void close() throws IOException {
    wrappedDocsReader.close();
  }
}
