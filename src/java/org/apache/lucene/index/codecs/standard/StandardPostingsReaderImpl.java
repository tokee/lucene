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
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Concrete class that reads the current doc/freq/skip
 *  postings format. */

// nocommit -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class StandardPostingsReaderImpl extends StandardPostingsReader {

  private final IndexInput freqIn;
  private final IndexInput proxIn;

  int skipInterval;
  int maxSkipLevels;

  public StandardPostingsReaderImpl(Directory dir, SegmentInfo segmentInfo, int readBufferSize) throws IOException {
    freqIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.FREQ_EXTENSION),
                           readBufferSize);
    if (segmentInfo.getHasProx()) {
      boolean success = false;
      try {
        proxIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.PROX_EXTENSION),
                               readBufferSize);
        success = true;
      } finally {
        if (!success) {
          freqIn.close();
        }
      }
    } else {
      proxIn = null;
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.FREQ_EXTENSION));
    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.PROX_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {

    // Make sure we are talking to the matching past writer
    Codec.checkHeader(termsIn, StandardPostingsWriterImpl.CODEC, StandardPostingsWriterImpl.VERSION_START);

    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
  }

  private static class DocTermState extends TermState {
    long freqOffset;
    long proxOffset;
    int skipOffset;

    public Object clone() {
      DocTermState other = (DocTermState) super.clone();
      other.freqOffset = freqOffset;
      other.proxOffset = proxOffset;
      other.skipOffset = skipOffset;
      return other;
    }

    public void copy(TermState _other) {
      super.copy(_other);
      DocTermState other = (DocTermState) _other;
      freqOffset = other.freqOffset;
      proxOffset = other.proxOffset;
      skipOffset = other.skipOffset;
    }

    public String toString() {
      return super.toString() + " freqFP=" + freqOffset + " proxFP=" + proxOffset + " skipOffset=" + skipOffset;
    }
  }

  @Override
  public TermState newTermState() {
    return new DocTermState();
  }

  @Override
  public void close() throws IOException {
    try {
      if (freqIn != null) {
        freqIn.close();
      }
    } finally {
      if (proxIn != null) {
        proxIn.close();
      }
    }
  }

  @Override
  public void readTerm(IndexInput termsIn, FieldInfo fieldInfo, TermState termState, boolean isIndexTerm)
    throws IOException {

    final DocTermState docTermState = (DocTermState) termState;

    if (Codec.DEBUG) {
      Codec.debug("  sdr.readTerm tis.fp=" + termsIn.getFilePointer() + " df=" + termState.docFreq + " isIndex?=" + isIndexTerm + " tis=" + termsIn);
    }

    if (isIndexTerm) {
      docTermState.freqOffset = termsIn.readVLong();
    } else {
      docTermState.freqOffset += termsIn.readVLong();
    }

    if (Codec.DEBUG) {
      Codec.debug("    frq.fp=" + docTermState.freqOffset + " vs len=" + freqIn.length());
    }

    if (docTermState.docFreq >= skipInterval) {
      docTermState.skipOffset = termsIn.readVInt();
    } else {
      docTermState.skipOffset = 0;
    }

    if (!fieldInfo.omitTermFreqAndPositions) {
      if (isIndexTerm) {
        docTermState.proxOffset = termsIn.readVLong();
      } else {
        docTermState.proxOffset += termsIn.readVLong();
      }
    }
  }
    
  @Override
  public DocsEnum docs(FieldInfo fieldInfo, TermState termState, Bits skipDocs, DocsEnum reuse) throws IOException {
    final SegmentDocsEnum docsEnum;
    if (reuse == null) {
      docsEnum = new SegmentDocsEnum(freqIn);
    } else {
      docsEnum = (SegmentDocsEnum) reuse;
    }
    return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, TermState termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    if (fieldInfo.omitTermFreqAndPositions) {
      return null;
    }
    final SegmentDocsAndPositionsEnum docsEnum;
    if (reuse == null) {
      docsEnum = new SegmentDocsAndPositionsEnum(freqIn, proxIn);
    } else {
      docsEnum = (SegmentDocsAndPositionsEnum) reuse;
    }
    return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
  }

  // Decodes only docs
  private class SegmentDocsEnum extends DocsEnum {
    final IndexInput freqIn;

    boolean omitTF;                               // does current field omit term freq?
    boolean storePayloads;                        // does current field store payloads?

    int limit;                                    // number of docs in this posting
    int ord;                                      // how many docs we've read
    int doc;                                      // doc we last read
    int freq;                                     // freq we last read

    Bits skipDocs;

    long freqOffset;
    int skipOffset;

    boolean skipped;
    DefaultSkipListReader skipper;

    public SegmentDocsEnum(IndexInput freqIn) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("new docs enum");
      }
      this.freqIn = (IndexInput) freqIn.clone();
    }

    public SegmentDocsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("[" + desc + "] dr.reset freqIn seek " + termState.freqOffset + " docCount=" + termState.docFreq);
      }
      omitTF = fieldInfo.omitTermFreqAndPositions;
      if (omitTF) {
        freq = 1;
      }
      storePayloads = fieldInfo.storePayloads;
      this.skipDocs = skipDocs;
      freqOffset = termState.freqOffset;
      skipOffset = termState.skipOffset;

      // nocommit this seek frequently isn't needed, when
      // we enum terms and all docs for each term (MTQ,
      // or, merging).  is this seek costing us anything?
      // we should avoid it so...
      freqIn.seek(termState.freqOffset);
      limit = termState.docFreq;
      ord = 0;
      doc = 0;

      skipped = false;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (Codec.DEBUG) {
        Codec.debug("sdr.next [" + desc + "] ord=" + ord + " vs df=" + limit + " freq.fp=" + freqIn.getFilePointer() + " + has skip docs=" + (skipDocs != null));
      }

      while(true) {
        if (ord == limit) {
          return doc = NO_MORE_DOCS;
        }

        ord++;

        // Decode next doc/freq pair
        final int code = freqIn.readVInt();
        if (Codec.DEBUG) {
          System.out.println("  read code=" + code);
        }
        if (omitTF) {
          doc += code;
        } else {
          doc += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = freqIn.readVInt();     // else read freq
          }
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        } else if (Codec.DEBUG) {
          System.out.println("  doc=" + doc + " is skipped");
        }
      }

      if (Codec.DEBUG) {
        System.out.println("  result doc=" + doc + " freq=" + freq);
      }

      return doc;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      if (Codec.DEBUG) {
        Codec.debug("sdr.bulk read: ord=" + ord + " df=" + limit + " omitTF=" + omitTF + " ord=" + ord + " of " + limit + " freq.fp=" + freqIn.getFilePointer(), desc);
      }
      int i = 0;
      final int length = docs.length;
      while (i < length && ord < limit) {
        ord++;
        // manually inlined call to next() for speed
        final int code = freqIn.readVInt();
        if (omitTF) {
          doc += code;
        } else {
          doc += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = freqIn.readVInt();     // else read freq
          }
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          if (Codec.DEBUG) {
            Codec.debug("  " + i + ": doc=" + doc + " freq=" + freq, desc);
          }
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

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
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
          // This is the first time this enum has ever been used for skipping -- do lazy init
          skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
        }

        if (!skipped) {

          // This is the first time this posting has
          // skipped since reset() was called, so now we
          // load the skip data for this posting

          skipper.init(freqOffset + skipOffset,
                       freqOffset, 0,
                       limit, storePayloads);

          if (Codec.DEBUG) {
            System.out.println("    skipper init  skipFP=" + (freqOffset+skipOffset) + " freqFP=" + freqOffset);
          }

          skipped = true;
        }

        final int newOrd = skipper.skipTo(target); 

        if (newOrd > ord) {
          // Skipper moved

          ord = newOrd;
          doc = skipper.getDoc();
          freqIn.seek(skipper.getFreqPointer());

          if (Codec.DEBUG) {
            System.out.println("dr [" + desc + "]: skipper moved to newOrd=" + newOrd + " freqFP=" + skipper.getFreqPointer() + " doc=" + doc + "; now scan...");
          }

        } else if (Codec.DEBUG) {
          System.out.println("  no skipping to be done");
        }
      } else if (Codec.DEBUG) {
        System.out.println("  no skip data (#docs is too low)");
      }
        
      // scan for the rest:
      do {
        nextDoc();
      } while (target > doc);

      return doc;
    }
  }

  // Decodes docs & positions
  private class SegmentDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private final IndexInput freqIn;
    private final IndexInput proxIn;

    boolean storePayloads;                        // does current field store payloads?

    int limit;                                    // number of docs in this posting
    int ord;                                      // how many docs we've read
    int doc;                                      // doc we last read
    int freq;                                     // freq we last read
    int position;

    Bits skipDocs;

    long freqOffset;
    int skipOffset;
    long proxOffset;

    int posPendingCount;
    int payloadLength;
    boolean payloadPending;

    boolean skipped;
    DefaultSkipListReader skipper;
    private BytesRef payload;
    private long lazyProxPointer;

    public SegmentDocsAndPositionsEnum(IndexInput freqIn, IndexInput proxIn) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("new docs enum");
      }
      this.freqIn = (IndexInput) freqIn.clone();
      this.proxIn = (IndexInput) proxIn.clone();
    }

    public SegmentDocsAndPositionsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("[" + desc + "] dr.init freqIn seek freq.fp=" + termState.freqOffset + " prox.fp=" + termState.proxOffset + " docCount=" + termState.docFreq);
      }
      assert !fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;
      if (storePayloads && payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[1];
      }

      this.skipDocs = skipDocs;

      // nocommit this seek frequently isn't needed, when
      // we enum terms and all docs for each term (MTQ,
      // or, merging).  is this seek costing us anything?
      // we should avoid it so...
      freqIn.seek(termState.freqOffset);
      lazyProxPointer = termState.proxOffset;

      limit = termState.docFreq;
      ord = 0;
      doc = 0;
      position = 0;

      skipped = false;
      posPendingCount = 0;
      payloadPending = false;

      freqOffset = termState.freqOffset;
      proxOffset = termState.proxOffset;
      skipOffset = termState.skipOffset;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (Codec.DEBUG) {
        Codec.debug("sdr.next [" + desc + "] ord=" + ord + " vs df=" + limit + " freq.fp=" + freqIn.getFilePointer() + " + has skip docs=" + (skipDocs != null));
      }

      while(true) {
        if (ord == limit) {
          return doc = NO_MORE_DOCS;
        }

        ord++;

        // Decode next doc/freq pair
        final int code = freqIn.readVInt();
        if (Codec.DEBUG) {
          System.out.println("  read code=" + code);
        }
        doc += code >>> 1;              // shift off low bit
        if ((code & 1) != 0) {          // if low bit is set
          freq = 1;                     // freq is one
        } else {
          freq = freqIn.readVInt();     // else read freq
        }
        posPendingCount += freq;

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        } else if (Codec.DEBUG) {
          System.out.println("  doc=" + doc + " is skipped");
        }
      }

      if (Codec.DEBUG) {
        System.out.println("  result doc=" + doc + " freq=" + freq);
      }
      position = 0;

      return doc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
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
          // This is the first time this enum has ever been used for skipping -- do lazy init
          skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
        }

        if (!skipped) {

          // This is the first time this posting has
          // skipped, since reset() was called, so now we
          // load the skip data for this posting

          skipper.init(freqOffset+skipOffset,
                       freqOffset, proxOffset,
                       limit, storePayloads);

          if (Codec.DEBUG) {
            Codec.debug("    skip reader base freqFP=" + (freqOffset+skipOffset) + " freqFP=" + freqOffset + " prox.fp=" + proxOffset);
          }

          skipped = true;
        }

        final int newOrd = skipper.skipTo(target); 

        if (newOrd > ord) {
          // Skipper moved
          ord = newOrd;
          doc = skipper.getDoc();
          freqIn.seek(skipper.getFreqPointer());
          lazyProxPointer = skipper.getProxPointer();
          posPendingCount = 0;
          position = 0;
          payloadPending = false;
          payloadLength = skipper.getPayloadLength();

          if (Codec.DEBUG) {
            Codec.debug("dr [" + desc + "]: skipper moved to newOrd=" + newOrd + " freq.fp=" + skipper.getFreqPointer() + " prox.fp=" + skipper.getProxPointer() + " doc=" + doc);
          }

        } else if (Codec.DEBUG) {
          System.out.println("  no skipping to be done");
        }
      } else if (Codec.DEBUG) {
        System.out.println("  no skip data (#docs is too low)");
      }
        
      // Now, linear scan for the rest:
      do {
        nextDoc();
      } while (target > doc);

      return doc;
    }

    public int nextPosition() throws IOException {

      if (lazyProxPointer != -1) {
        proxIn.seek(lazyProxPointer);
        lazyProxPointer = -1;
      }
      
      if (Codec.DEBUG) {
        System.out.println("nextPos [" + desc + "] payloadPending=" + payloadPending + " payloadLen=" + payloadLength + " posPendingCount=" + posPendingCount + " freq=" + freq);
      }

      if (payloadPending && payloadLength > 0) {
        // payload of last position as never retrieved -- skip it
        if (Codec.DEBUG) {
          System.out.println("      skip payload len=" + payloadLength);
        }
        proxIn.seek(proxIn.getFilePointer() + payloadLength);
        payloadPending = false;
      }

      // scan over any docs that were iterated without their positions
      while(posPendingCount > freq) {

        if (Codec.DEBUG) {
          System.out.println("      skip position");
        }

        final int code = proxIn.readVInt();

        if (storePayloads) {
          if ((code & 1) != 0) {
            // new payload length
            payloadLength = proxIn.readVInt();
            assert payloadLength >= 0;
            if (Codec.DEBUG) {
              System.out.println("        new payloadLen=" + payloadLength);
            }
          }
          assert payloadLength != -1;
          proxIn.seek(proxIn.getFilePointer() + payloadLength);
          if (Codec.DEBUG) {
            System.out.println("        skip payloadLen=" + payloadLength + " bytes");
          }
        }

        posPendingCount--;
        position = 0;
        payloadPending = false;
      }

      // read next position
      if (storePayloads) {

        if (payloadPending && payloadLength > 0) {
          // payload wasn't retrieved for last position
          if (Codec.DEBUG) {
            System.out.println("      payload pending: skip " + payloadLength + " bytes");
          }
          proxIn.seek(proxIn.getFilePointer()+payloadLength);
        }

        final int code = proxIn.readVInt();
        if ((code & 1) != 0) {
          // new payload length
          payloadLength = proxIn.readVInt();
          assert payloadLength >= 0;
          if (Codec.DEBUG) {
            System.out.println("      new payloadLen=" + payloadLength);
          }
        }
        assert payloadLength != -1;
          
        payloadPending = true;
        position += code >>> 1;
      } else {
        position += proxIn.readVInt();
      }

      posPendingCount--;

      assert posPendingCount >= 0: "nextPosition() was called too many times (more than freq() times) posPendingCount=" + posPendingCount;

      if (Codec.DEBUG) {
        System.out.println("   proxFP=" + proxIn.getFilePointer() + " return pos=" + position);
      }
      return position;
    }

    /** Returns length of payload at current position */
    public int getPayloadLength() {
      assert lazyProxPointer == -1;
      assert posPendingCount < freq;
      return payloadLength;
    }

    /** Returns the payload at this position, or null if no
     *  payload was indexed. */
    public BytesRef getPayload() throws IOException {
      assert lazyProxPointer == -1;
      assert posPendingCount < freq;
      if (Codec.DEBUG) {
        System.out.println("      read payload: " + payloadLength);
      }
      if (!payloadPending) {
        throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
      }
      if (payloadLength > payload.bytes.length) {
        payload.grow(payloadLength);
      }
      proxIn.readBytes(payload.bytes, 0, payloadLength);
      payload.length = payloadLength;
      payloadPending = false;

      return payload;
    }

    public boolean hasPayload() {
      return payloadPending && payloadLength > 0;
    }
  }
}
