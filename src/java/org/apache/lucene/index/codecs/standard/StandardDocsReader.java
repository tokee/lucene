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

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.index.codecs.DocsProducer;

/** Concrete class that reads the current doc/freq/skip
 *  postings format */

// nocommit -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class StandardDocsReader extends DocsProducer {

  final IndexInput freqIn;
  IndexInput termsIn;

  private final StandardPositionsReader posReader;

  int skipInterval;
  int maxSkipLevels;

  public StandardDocsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize) throws IOException {
    freqIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.FREQ_EXTENSION), readBufferSize);

    boolean success = false;
    try {
      if (segmentInfo.getHasProx()) {
        posReader = new StandardPositionsReader(dir, segmentInfo, readBufferSize);
      } else {
        posReader = null;
      }
      // mxx
      if (Codec.DEBUG) {
        System.out.println(Thread.currentThread().getName() + ": sdr.init: hasProx=" + segmentInfo.getHasProx() + " posReader=" + posReader + " seg=" + segmentInfo.name + " docCount=" + segmentInfo.docCount);
      }
      success = true;
    } finally {
      if (!success) {
        freqIn.close();
      }
    }
  }

  public static void files(SegmentInfo segmentInfo, Collection files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.FREQ_EXTENSION));
    StandardPositionsReader.files(segmentInfo, files);
  }

  public void start(IndexInput termsIn) throws IOException {
    this.termsIn = termsIn;

    // Make sure we are talking to the matching past writer
    Codec.checkHeader(termsIn, StandardDocsWriter.CODEC, StandardDocsWriter.VERSION_START);

    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    if (posReader != null)
      posReader.start(termsIn);
  }

  public Reader reader(FieldInfo fieldInfo, IndexInput termsIn) {

    final StandardPositionsReader.TermsDictReader posReader2;
    if (posReader != null && !fieldInfo.omitTermFreqAndPositions) {
      posReader2 = (StandardPositionsReader.TermsDictReader) posReader.reader(fieldInfo, termsIn);
    } else {
      posReader2 = null;
    }

    return new TermsDictReader(fieldInfo, posReader2, termsIn);
  }

  public void close() throws IOException {
    try {
      freqIn.close();
    } finally {
      if (posReader != null) {
        posReader.close();
      }
    }
  }

  class TermsDictReader extends Reader {

    final IndexInput termsIn;
    final FieldInfo fieldInfo;
    long freqOffset;
    long skipOffset;
    int docFreq;

    // TODO: abstraction violation (we are storing this with
    // the concrete impl. as the type, not the abstract base
    // class)
    final StandardPositionsReader.TermsDictReader posReader;
    private SegmentDocsEnum docs;

    TermsDictReader(FieldInfo fieldInfo, StandardPositionsReader.TermsDictReader posReader, IndexInput termsIn) {
      this.termsIn = termsIn;                     // not cloned
      this.fieldInfo = fieldInfo;
      this.posReader = posReader;
      if (Codec.DEBUG) {
        System.out.println("sdr.tdr: init");
      }
    }

    public void readTerm(int docFreq, boolean isIndexTerm) throws IOException {

      this.docFreq = docFreq;
      // mxx
      if (Codec.DEBUG) {
        System.out.println("  sdr.readTerm termsInPointer=" + termsIn.getFilePointer() + " df=" + docFreq + " isIndex?=" + isIndexTerm + " posReader=" + posReader);
      }

      if (isIndexTerm) {
        freqOffset = termsIn.readVLong();
      } else {
        freqOffset += termsIn.readVLong();
      }

      // mxx
      if (Codec.DEBUG) {
        System.out.println("    freqOffset=" + freqOffset + " vs len=" + freqIn.length());
      }

      if (docFreq >= skipInterval) {
        skipOffset = termsIn.readVLong();
      } else {
        skipOffset = 0;
      }

      if (posReader != null) {
        posReader.readTerm(docFreq, isIndexTerm);
      }
    }
    
    public class TermDictsReaderState extends State {
      long termsInPos;
      long freqOffset;
      long skipOffset;
      long freqInPos;
      int freq;
      long proxPos;
      public long proxOffset;
    }
    
    @Override
    public State captureState(State reusableState) {
      TermDictsReaderState state;
      if(reusableState == null) {
        state = new TermDictsReaderState();
      } else {
        state = (TermDictsReaderState) reusableState;
        state.proxPos = 0;
        state.proxOffset = 0;
      }
      if(posReader != null) {
        if(posReader.positions != null) {
          state.proxPos = posReader.positions.proxIn.getFilePointer();
        }
        state.proxOffset = posReader.proxOffset;
      }
      state.termsInPos = termsIn.getFilePointer();
      state.freqOffset = freqOffset;
      state.freqInPos = freqIn.getFilePointer();
      state.freq = docFreq;
      state.skipOffset = skipOffset;
      return state;
    }

    @Override
    public void setState(State state) throws IOException {
      TermDictsReaderState readerState = (TermDictsReaderState)state;
      skipOffset = readerState.skipOffset;
      termsIn.seek(readerState.termsInPos);
      freqOffset = readerState.freqOffset;
      freqIn.seek(readerState.freqInPos);
      docFreq = readerState.freq;
      
      if(posReader != null) {
        if(posReader.positions != null) {
          posReader.positions.proxIn.seek(readerState.proxPos);
        }
        posReader.proxOffset = readerState.proxOffset;
      }
    }
    
    public boolean canCaptureState() {
      return true;
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
      long skipStart;
      long freqStart;
      final IndexInput freqIn;
      // nocommit -- should we do omitTF with 2 different enum classes?
      final boolean omitTF;
      private Bits skipDocs;

      // nocommit -- should we do hasProx with 2 different enum classes?

      boolean skipped;
      DefaultSkipListReader skipper;

      // TODO: abstraction violation: we are storing the
      // concrete impl, not the abstract base class
      StandardPositionsReader.TermsDictReader.SegmentPositionsEnum positions;

      SegmentDocsEnum() {
        if (Codec.DEBUG) {
          System.out.println("new docs enum");
        }
        this.freqIn = (IndexInput) StandardDocsReader.this.freqIn.clone();
        omitTF = fieldInfo.omitTermFreqAndPositions;
        if (omitTF) {
          freq = 1;
        }
      }

      void init(Bits skipDocs) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("[" + desc + "] dr.init freqIn seek " + freqOffset + " this=" + this + " (in=" + freqIn + "; this=" + this + ") docFreq=" + TermsDictReader.this.docFreq);
        }
        this.skipDocs = skipDocs;
        freqIn.seek(freqOffset);
        this.docFreq = TermsDictReader.this.docFreq;
        count = 0;
        doc = 0;
        skipped = false;
        skipStart = freqStart + skipOffset;
        proxSkipFreq = 0;

        // maybe not necessary?
        proxSkipPayloadLength = -1;

        // nocommit: abstraction violation
        if (posReader != null) {
          proxOffset = posReader.proxOffset;
        }

        if (positions != null) {
          positions.payloadLength = -1;
        }
        //new Throwable().printStackTrace(System.out);
      }

      public int next() throws IOException {
        if (Codec.DEBUG) {
          System.out.println("sdr.next [" + desc + "] count=" + count + " vs df=" + docFreq + " freq pointer=" + freqIn.getFilePointer() + " (in=" + freqIn + "; this=" + this + ") + has skip docs=" + (skipDocs != null));
        }

        while(true) {
          if (count == docFreq) {
            return NO_MORE_DOCS;
          }

          count++;

          // Decode next doc/freq pair
          final int code = freqIn.readVInt();
          if (Codec.DEBUG) {
            System.out.println("  read code=" + code);
          }
          if (omitTF)
            doc += code;
          else {
            doc += code >>> 1;              // shift off low bit
            if ((code & 1) != 0)            // if low bit is set
              freq = 1;                     // freq is one
            else
              freq = freqIn.readVInt();     // else read freq

            if (positions != null)
              positions.skip(freq);
            else
              proxSkipFreq += freq;
          }

          if (skipDocs == null || !skipDocs.get(doc)) {
            break;
          } else if (Codec.DEBUG) {
            System.out.println("  doc=" + doc + " is skipped");
          }
        }

        // nocommit
        if (Codec.DEBUG && positions != null) {
          positions.desc = desc + ":" + doc;
        }

        if (Codec.DEBUG) {
          System.out.println("  result doc=" + doc);
        }
        return doc;
      }

      public int read(int[] docs, int[] freqs) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("sdr.read: count=" + count + " df=" + docFreq);
        }
        int i = 0;
        final int length = docs.length;
        while (i < length && count < docFreq) {
          count++;
          // manually inlined call to next() for speed
          final int code = freqIn.readVInt();
          if (omitTF) {
            doc += code;
            freq = 1;
          } else {
            doc += code >>> 1;              // shift off low bit
            if ((code & 1) != 0)            // if low bit is set
              freq = 1;                     // freq is one
            else
              freq = freqIn.readVInt();     // else read freq

            if (positions != null)
              positions.skip(freq);
            else
              proxSkipFreq += freq;
          }

          if (skipDocs == null || !skipDocs.get(doc)) {
            docs[i] = doc;
            freqs[i] = freq;
            ++i;
          }
        }
        if (Codec.DEBUG) {
          System.out.println("  return " + i);
        }

        return i;
      }

      public int doc() {
        return doc;
      }

      public int freq() {
        return freq;
      }

      long proxOffset;
      int proxSkipPayloadLength = -1;
      int proxSkipFreq;
      PositionsEnum fakePositions;

      public PositionsEnum positions() throws IOException {
        if (Codec.DEBUG) {
          System.out.println("str.positions: create");
        }
        if (positions == null) {
          // Lazy init
          if (posReader == null) {
            // TermFreq was omitted from this field during
            // indexing, which means we pretend termFreq is
            // always 1 with that 1 occurrence having
            // position 0
            if (fakePositions == null)
              fakePositions = new FormatPostingsFakePositionsEnum();
            return fakePositions;
          } else {
            // TODO: abstraction violation
            positions = (StandardPositionsReader.TermsDictReader.SegmentPositionsEnum) posReader.positions();
            if (Codec.DEBUG) {
              System.out.println("pos skip proxOffset=" + proxOffset + " payloadlen=" + proxSkipPayloadLength + " skipPosCount= " + proxSkipFreq);
            }
            positions.skip(proxOffset, proxSkipPayloadLength, proxSkipFreq);
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
          System.out.println("dr [" + desc + "]: skip to target=" + target);
        }

        if (skipOffset > 0) {

          // There are enough docs in the posting to have
          // skip data
          if (skipper == null) {
            // Lazy init
            skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
          }

          if (!skipped) {

            // We haven't already skipped for this posting,
            // so now we init the skipper

            // TODO: this is abstraction violation; instead,
            // skipper should interact with this as a
            // private consumer
            skipper.init(freqOffset+skipStart,
                         freqOffset, proxOffset,
                         docFreq, fieldInfo.storePayloads);

            if (Codec.DEBUG) {
              System.out.println("    skip reader base freqFP=" + (freqOffset+skipStart) + " freqFP=" + freqOffset + " proxFP=" + proxOffset);
            }

            skipped = true;
          }

          final int newCount = skipper.skipTo(target); 

          if (newCount > count) {

            if (Codec.DEBUG) {
              System.out.println("dr [" + desc + "]: skipper moved to newCount=" + newCount + " freqFP=" + skipper.getFreqPointer() + " proxFP=" + skipper.getProxPointer() + " doc=" + skipper.getDoc());
            }

            // Skipper did move
            freqIn.seek(skipper.getFreqPointer());
            count = newCount;
            doc = skipper.getDoc();

            // TODO: abstraction violation; this should be a
            // private interaction b/w skipper & posReader
            if (positions != null) {
              // nocommit -- should that be count?
              positions.skip(skipper.getProxPointer(), skipper.getPayloadLength(), 0);
            } else {
              proxOffset = skipper.getProxPointer();
              proxSkipPayloadLength = skipper.getPayloadLength();
              // nocommit -- should that be count?
              proxSkipFreq = 0;
            }
          } else if (Codec.DEBUG) {
            System.out.println("  no skipping to be done");
          }
        } else if (Codec.DEBUG) {
          System.out.println("  no skip data (#docs is too low)");
        }
        
        // Now, linear scan for the rest:
        do {
          if (next() == NO_MORE_DOCS)
            return NO_MORE_DOCS;
        } while (target > doc);

        return doc;
      }
    }
  }
}

/** Returned when someone asks for positions() enum on field
 *  with omitTf true */
class FormatPostingsFakePositionsEnum extends PositionsEnum {
  @Override
  public int next() {
    return 0;
  }
  @Override
  public int getPayloadLength() {
    return 0;
  }
  @Override
  public boolean hasPayload() {
    return false;
  }
  @Override
  public byte[] getPayload(byte[] data, int offset) {
    return null;
  }
}
