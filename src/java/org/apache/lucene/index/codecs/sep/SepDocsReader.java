package org.apache.lucene.index.codecs.sep;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.codecs.DocsProducer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.codecs.Codec;

/** Concrete class that reads the current doc/freq/skip
 *  postings format */

// nocommit -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class SepDocsReader extends DocsProducer {

  final IntIndexInput freqIn;
  final IntIndexInput docIn;

  final IndexInput skipIn;

  IndexInput termsIn;

  private final SepPositionsReader posReader;

  int skipInterval;
  int maxSkipLevels;

  public SepDocsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize, IntStreamFactory intFactory) throws IOException {

    boolean success = false;
    try {

      // nocommit -- freqIn is null if omitTF?
      final String frqFileName = IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.FREQ_EXTENSION);
      freqIn = intFactory.openInput(dir, frqFileName);

      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.DOC_EXTENSION);
      docIn = intFactory.openInput(dir, docFileName);

      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.SKIP_EXTENSION), readBufferSize);
      if (segmentInfo.getHasProx()) {
        final String posFileName = IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.POS_EXTENSION);
        posReader = new SepPositionsReader(dir, segmentInfo, readBufferSize, intFactory);
      } else {
        posReader = null;
      }
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
  }

  public static void files(SegmentInfo segmentInfo, Collection files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.FREQ_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.DOC_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.SKIP_EXTENSION));
    SepPositionsReader.files(segmentInfo, files);
  }

  public void start(IndexInput termsIn) throws IOException {
    this.termsIn = termsIn;

    // Make sure we are talking to the matching past writer
    Codec.checkHeader(termsIn, SepDocsWriter.CODEC, SepPositionsWriter.VERSION_START);

    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    if (posReader != null) {
      posReader.start(termsIn);
    }
  }

  public Reader reader(FieldInfo fieldInfo, IndexInput termsIn) throws IOException {

    final SepPositionsReader.TermsDictReader posReader2;
    if (posReader != null && !fieldInfo.omitTermFreqAndPositions) {
      posReader2 = (SepPositionsReader.TermsDictReader) posReader.reader(fieldInfo, termsIn);
    } else {
      posReader2 = null;
    }

    return new TermsDictReader(fieldInfo, posReader2, termsIn);
  }

  public void close() throws IOException {
    try {
      if (freqIn != null)
        freqIn.close();
    } finally {
      try {
        if (docIn != null)
          docIn.close();
      } finally {
        try {
          if (skipIn != null)
            skipIn.close();
        } finally {
          if (posReader != null)
            posReader.close();
        }
      }
    }
  }

  class TermsDictReader extends Reader {

    final IndexInput termsIn;
    final FieldInfo fieldInfo;
    final IntIndexInput.Reader freqIn;
    final IntIndexInput.Index freqIndex;
    final IntIndexInput.Reader docIn;
    final IntIndexInput.Index docIndex;
    final private boolean omitTF;

    long skipOffset;
    int docFreq;

    // TODO: abstraction violation (we are storing this with
    // the concrete impl. as the type, not the abstract base
    // class)
    final SepPositionsReader.TermsDictReader posReader;
    private SegmentDocsEnum docs;

    TermsDictReader(FieldInfo fieldInfo, SepPositionsReader.TermsDictReader posReader, IndexInput termsIn) throws IOException {
      this.termsIn = termsIn;                     // not cloned
      this.fieldInfo = fieldInfo;
      this.posReader = posReader;
      this.docIn = SepDocsReader.this.docIn.reader();
      docIndex = SepDocsReader.this.docIn.index();
      omitTF = fieldInfo.omitTermFreqAndPositions;
      if (!omitTF) {
        this.freqIn = SepDocsReader.this.freqIn.reader();
        freqIndex = SepDocsReader.this.freqIn.index();
      } else {
        this.freqIn = null;
        freqIndex = null;
        docFreq = 1;
      }
    }

    public void readTerm(int docFreq, boolean isIndexTerm) throws IOException {

      this.docFreq = docFreq;
      if (Codec.DEBUG) {
        System.out.println("  dr.readTerm termsFP=" + termsIn.getFilePointer() + " df=" + docFreq + " isIndex=" + isIndexTerm);
        System.out.println("    start freqFP=" + freqIndex + " docFP=" + docIndex + " skipFP=" + skipOffset);
      }

      if (!omitTF) {
        freqIndex.read(termsIn, isIndexTerm);
      }

      docIndex.read(termsIn, isIndexTerm);

      if (isIndexTerm) {
        skipOffset = termsIn.readVLong();
      } else {
        if (docFreq >= skipInterval) {
          skipOffset += termsIn.readVLong();
        }
      }

      if (Codec.DEBUG) {
        System.out.println("    freqFP=" + freqIndex + " docFP=" + docIndex + " skipFP=" + skipOffset);
      }

      if (posReader != null) {
        posReader.readTerm(docFreq, isIndexTerm);
      }
    }

    public DocsEnum docs(Bits skipDocs) throws IOException {

      if (docs == null) {
        // Lazy init
        docs = new SegmentDocsEnum();
      }

      docs.init(skipDocs);

      return docs;
    }

    class SegmentDocsEnum extends DocsEnum {
      int docFreq;
      int doc;
      int count;
      int freq;
      long freqStart;

      // nocommit -- should we do omitTF with 2 different enum classes?
      final boolean omitTF;
      private Bits skipDocs;

      // nocommit -- should we do hasProx with 2 different enum classes?

      boolean skipped;
      SepSkipListReader skipper;

      // TODO: abstraction violation: we are storing the
      // concrete impl, not the abstract base class
      SepPositionsReader.TermsDictReader.SegmentPositionsEnum positions;

      SegmentDocsEnum() {
        if (Codec.DEBUG) {
          System.out.println("new docs enum");
        }
        omitTF = fieldInfo.omitTermFreqAndPositions;
        if (omitTF) {
          freq = 1;
        }
      }

      void init(Bits skipDocs) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("[" + desc + "] dr.init freqIn seek " + freqIndex + " this=" + this + " (in=" + freqIn + "; this=" + this + ")");
        }
        this.skipDocs = skipDocs;

        // nocommit: can't we only do this if consumer
        // skipped consuming the previous docs?
        docIndex.seek(docIn);

        if (!omitTF) {
          freqIndex.seek(freqIn);
        }
        this.docFreq = TermsDictReader.this.docFreq;
        count = 0;
        doc = 0;
        skipped = false;
        proxSkipFreq = 0;

        // maybe not necessary?
        proxSkipPayloadLength = -1;

        // TODO: abstraction violation
        if (posReader != null) {
          //posIndex = posReader.posIndex;
          posIndex = posReader.getPosIn().index();
          posIndex.set(posReader.posIndex);
          payloadOffset = posReader.payloadOffset;
        }
      }

      public int next() throws IOException {

        if (Codec.DEBUG) {
          if (!omitTF) {
            System.out.println("sdr [" + desc + "] next count=" + count + " vs df=" + docFreq + " freqFP=" + freqIn.descFilePointer() + " docFP=" + docIn.descFilePointer() + " skipDocs?=" + (skipDocs != null) );
          } else {
            System.out.println("sdr [" + desc + "] next count=" + count + " vs df=" + docFreq + " docFP=" + docIn.descFilePointer() + " skipDocs?=" + (skipDocs != null) );
          }
        }

        while(true) {
          if (count == docFreq) {
            return NO_MORE_DOCS;
          }

          count++;

          // Decode next doc
          doc += docIn.next();
          
          if (!omitTF) {
            freq = freqIn.next();
            if (positions != null) {
              positions.seek(freq);
            } else {
              proxSkipFreq += freq;
            }
          }

          if (Codec.DEBUG) {
            System.out.println("  decode doc=" + doc + " freq=" + freq);
          }

          if (skipDocs == null || !skipDocs.get(doc)) {
            break;
          } else if (Codec.DEBUG) {
            System.out.println("  doc=" + doc + " is skipped");
          }
        }

        // nocommit
        if (Codec.DEBUG) {
          if (positions != null) {
            positions.desc = desc + ":" + doc;
          }
          System.out.println("  return doc=" + doc);
        }
        return doc;
      }

      public int read(int[] docs, int[] freqs) throws IOException {
        // nocommit -- switch to bulk read api in IntIndexInput
        int i = 0;
        final int length = docs.length;
        while (i < length && count < docFreq) {
          count++;
          // manually inlined call to next() for speed
          doc += docIn.next();
          if (!omitTF) {
            freq = freqIn.next();
            if (positions != null) {
              positions.seek(freq);
            } else {
              proxSkipFreq += freq;
            }
          }

          if (skipDocs == null || !skipDocs.get(doc)) {
            docs[i] = doc;
            freqs[i] = freq;
            i++;
          }
        }

        return i;
      }

      public int freq() {
        return freq;
      }

      // Holds pending seek data for positions:
      IntIndexInput.Index posIndex;
      long payloadOffset;
      int proxSkipPayloadLength;

      // If we step through docs w/o getting positions for
      // them, we accumulate how many freqs we've skipped
      // here.  Then, when positions() is called, we skip
      // this many positions to catch up:
      int proxSkipFreq;

      PositionsEnum fakePositions;

      public PositionsEnum positions() throws IOException {
        
        if (Codec.DEBUG) {
          System.out.println("sep.positions pos=" + positions + " freq=" + freq);
        }

        if (positions == null) {

          // First time positions is requested from this DocsEnum

          // Lazy init
          if (posReader == null) {

            // nocommit -- should we return null?

            // TermFreq was omitted from this field during
            // indexing, which means we pretend termFreq is
            // always 1 with that 1 occurrence having
            // position 0
            if (fakePositions == null) {
              fakePositions = new FakePositionsEnum();
            }
            if (Codec.DEBUG) {
              System.out.println("  return fake");
            }
            return fakePositions;
          } else {

            // nocommit: abstraction violation
            positions = (SepPositionsReader.TermsDictReader.SegmentPositionsEnum) posReader.positions();
            if (Codec.DEBUG) {
              System.out.println("pos skip posIndex=" + posIndex + " payloadlen=" + proxSkipPayloadLength + " skipPosCount= " + proxSkipFreq);
            }
            positions.seek(posIndex, payloadOffset, proxSkipPayloadLength);

            // TODO: technically, if this positions is deep
            // into the DocsEnum iteration, it'd pay to use
            // the skipper to catch up, instead of linear
            // scan:
            positions.seek(proxSkipFreq);
            proxSkipFreq = 0;
          }
        }

        if (Codec.DEBUG) {
          positions.desc = desc + ":" + doc;
        }

        positions.catchUp(freq);

        return positions;
      }

      public int advance(int target) throws IOException {

        // TODO: jump right to next() if target is < X away
        // from where we are now?

        if (Codec.DEBUG) {
          System.out.println("sdr [" + desc + "]: advance target=" + target);
        }

        if (docFreq >= skipInterval) {

          // There are enough docs in the posting to have
          // skip data
          if (skipper == null) {
            // Lazy init
            if (Codec.DEBUG) {
              System.out.println("  create skipper");
            }
            skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
                                            omitTF ? null : SepDocsReader.this.freqIn,
                                            SepDocsReader.this.docIn,
                                            posReader == null ? null : posReader.getPosIn(),
                                            maxSkipLevels, skipInterval);
          }

          if (!skipped) {

            // We haven't yet skipped for this posting,
            // so now we init the skipper

            // TODO: this is abstraction violation; instead,
            // skipper should interact with this as a
            // private consumer
            skipper.init(skipOffset,
                         docIndex,
                         freqIndex,
                         posReader != null ? posReader.posIndex : null,
                         payloadOffset,
                         docFreq,
                         fieldInfo.storePayloads);

            if (Codec.DEBUG) {
              System.out.println("    init skipper: base skipFP=" + skipOffset + " docFP=" + docIndex + " freqFP=" + freqIndex + " proxFP=" +
                                 (posReader != null ? posReader.posIndex : null) + " payloadFP=" + payloadOffset);
            }

            skipped = true;
          }

          final int newCount = skipper.skipTo(target); 

          if (newCount > count) {

            if (Codec.DEBUG) {
              System.out.println("sdr [" + desc + "]: skipper moved to newCount=" + newCount +
                                 " docFP=" + skipper.getDocIndex() +
                                 " freqFP=" + skipper.getFreqIndex() +
                                 " posFP=" + skipper.getPosIndex() +
                                 " payloadFP=" + skipper.getPayloadPointer() +
                                 " doc=" + skipper.getDoc());
            }
            
            // Skipper did move
            if (!omitTF) {
              skipper.getFreqIndex().seek(freqIn);
            }
            skipper.getDocIndex().seek(docIn);
            count = newCount;
            doc = skipper.getDoc();

            // TODO: abstraction violation; this should be a
            // private interaction b/w skipper & posReader
            if (positions != null) {
              positions.seek(skipper.getPosIndex(),
                             skipper.getPayloadPointer(),
                             skipper.getPayloadLength());
            } else {
              if (posIndex != null) {
                posIndex.set(skipper.getPosIndex());
              }
              payloadOffset = skipper.getPayloadPointer();
              proxSkipPayloadLength = skipper.getPayloadLength();
              proxSkipFreq = 0;
            }
          } else if (Codec.DEBUG) {
            System.out.println("  no skipping to be done");
          }
        }
        
        // Now, linear scan for the rest:
        do {
          if (next() == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          }
        } while (target > doc);

        return doc;
      }
    }

    @Override
    public State captureState(State reusableState) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setState(State state) throws IOException {
      // TODO Auto-generated method stub
      
    }
  }
}

/** Returned when someone asks for positions() enum on field
 *  with omitTf true */
class FakePositionsEnum extends PositionsEnum {
  public int next() {
    return 0;
  }
  public int getPayloadLength() {
    return 0;
  }
  public boolean hasPayload() {
    return false;
  }
  public byte[] getPayload(byte[] data, int offset) {
    return null;
  }
}
