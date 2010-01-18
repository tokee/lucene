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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.standard.StandardPositionsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

/** @lucene.experimental */
public class SepPositionsReader extends StandardPositionsProducer {
  
  final IntIndexInput posIn;

  final IndexInput payloadIn;

  IndexInput termsIn;

  public SepPositionsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize, IntStreamFactory intFactory) throws IOException {
    assert segmentInfo.getHasProx();
    boolean success = false;
    try {
      posIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.POS_EXTENSION), readBufferSize);
      payloadIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.PAYLOAD_EXTENSION), readBufferSize);
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
  }

  @Override
  public void start(IndexInput termsIn) throws IOException {
    this.termsIn = termsIn;

    // nocomit -- move these 2 constants into XXXCodec?
    Codec.checkHeader(termsIn, SepPositionsWriter.CODEC, SepPositionsWriter.VERSION_START);
  }

  static void files(SegmentInfo segmentInfo, Collection<String> files) {
    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.POS_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, SepCodec.PAYLOAD_EXTENSION));
    }
  }

  @Override
  public Reader reader(FieldInfo fieldInfo, IndexInput termsIn) throws IOException {
    return new TermsDictReader(termsIn, fieldInfo);
  }

  @Override
  public void close() throws IOException {
    try {
      if (posIn != null)
        posIn.close();
    } finally {
      if (payloadIn != null)
        payloadIn.close();
    }
  }

  class TermsDictReader extends Reader {

    final IndexInput termsIn;
    final IntIndexInput.Reader posIn;
    final IntIndexInput.Index posIndex;
    
    final FieldInfo fieldInfo;
    long payloadOffset;

    TermsDictReader(IndexInput termsIn, FieldInfo fieldInfo) throws IOException {
      this.termsIn = termsIn;
      this.fieldInfo = fieldInfo;
      this.posIn = SepPositionsReader.this.posIn.reader();
      posIndex = SepPositionsReader.this.posIn.index();
    }

    public IntIndexInput getPosIn() {
      return SepPositionsReader.this.posIn;
    }

    @Override
    public void readTerm(int docFreq, boolean isIndexTerm) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("    pr.readterm termsInPointer=" + termsIn.getFilePointer() + " isIndex=" + isIndexTerm);
      }
      posIndex.read(termsIn, isIndexTerm);
      if (isIndexTerm) {
        payloadOffset = termsIn.readVLong();
      } else {
        payloadOffset += termsIn.readVLong();
      }
      if (Codec.DEBUG) {
        System.out.println("      posIndex=" + posIndex + " payloadOffset=" + payloadOffset);
      }
      if (positions != null) {
        positions.seek(posIndex, payloadOffset, -1);
      }
    }

    SegmentPositionsEnum positions;

    @Override
    public PositionsEnum positions() throws IOException {

      if (positions == null) {
        // Lazy init
        positions = new SegmentPositionsEnum(posIndex, payloadOffset);
      }

      return positions;
    }

    // nocommit -- should we have different reader for
    // payload vs no payload?
    class SegmentPositionsEnum extends PositionsEnum {

      // nocommit
      String desc;

      //final IntIndexInput posIn;
      final IndexInput payloadIn;
      final IntIndexInput.Index pendingPosIndex;

      final boolean storePayloads;

      boolean payloadPending;                     // True if we must skip payload before reading next position

      long payloadOffset;

      int position;
      int payloadLength;
      int posSkipCount;

      boolean seekPending;

      SegmentPositionsEnum(IntIndexInput.Index posIndex, long payloadOffset) throws IOException {
        //posIn = SepPositionsReader.this.posIn.reader();
        this.payloadOffset = payloadOffset;
        pendingPosIndex = SepPositionsReader.this.posIn.index();
        pendingPosIndex.set(posIndex);
        seekPending = true;

        if (Codec.DEBUG) {
          System.out.println("new pos enum seekPending=true posIndex=" + pendingPosIndex);
        }
        storePayloads = fieldInfo.storePayloads;
        if (storePayloads) {
          payloadIn = (IndexInput) SepPositionsReader.this.payloadIn.clone();
        } else {
          payloadIn = null;
        }
      }

      public void seek(IntIndexInput.Index posIndex, long payloadOffset, int payloadLength) {
        if (Codec.DEBUG) {
          System.out.println("spr.seek posIndex=" + posIndex);
        }
        pendingPosIndex.set(posIndex);
        this.payloadOffset = payloadOffset;
        this.payloadLength = payloadLength;
        posSkipCount = 0;
        seekPending = true;
      }

      // Cumulative on top of a previons Index seek
      public void seek(int posCount) {
        posSkipCount += posCount;
        if (Codec.DEBUG) {
          System.out.println("pr [" + desc + "] skip " + posCount + " positions; now " + posSkipCount);
        }
      }

      void catchUp(int currentCount) throws IOException {
        if (Codec.DEBUG) {
          System.out.println("pos catchup [" + desc + "]: seekPending=" + seekPending + " seekPosIndex=" + pendingPosIndex + " payloadPending=" + payloadPending + " payloadFP=" + payloadOffset + " skipPosCount " + posSkipCount + " vs currentCount " + currentCount);
        }

        if (seekPending) {
          pendingPosIndex.seek(posIn);
          if (storePayloads) {
            payloadIn.seek(payloadOffset);
          }
          payloadPending = false;
          seekPending = false;
        }

        while(posSkipCount > currentCount) {
          next();
        }

        if (Codec.DEBUG) {
          System.out.println("  pos catchup done");
        }
        position = 0;
      }

      @Override
      public int next() throws IOException {

        if (Codec.DEBUG) {
          System.out.println("pr.next [" + desc + "]: posFP=" + posIn.descFilePointer() + getPayloadFP());
        }

        final int code = posIn.next();

        if (storePayloads) {

          if (payloadPending && payloadLength > 0) {
            if (Codec.DEBUG) {
              System.out.println("  payload pending: skip " + payloadLength + " bytes");
            }
            // nocommit: do this lazily, when getPayload()
            // is called
            payloadIn.seek(payloadIn.getFilePointer()+payloadLength);
          }

          if ((code & 1) != 0) {
            // Payload length has changed
            payloadLength = posIn.next();
            assert payloadLength >= 0;
            if (Codec.DEBUG) {
              System.out.println("  new payloadLen=" + payloadLength);
            }
          }
          assert payloadLength != -1;
          
          payloadPending = true;
          position += code >>> 1;
        } else {
          position += code;
        }

        posSkipCount--;

        // NOTE: the old API actually allowed this... and some tests actually did it
        assert posSkipCount >= 0: "next() was called too many times (more than FormatPostingsDocsEnum.freq() times)";

        if (Codec.DEBUG) {
          System.out.println("  proxFP=" + posIn.descFilePointer() + getPayloadFP() + " return pos=" + position);
        }

        return position;
      }

      // debugging only
      private String getPayloadFP() {
        if (payloadIn != null) {
          return " payloadFP=" + payloadIn.getFilePointer();
        } else {
          return " payloadFP=null";
        }
      }

      @Override
      public int getPayloadLength() {
        return payloadLength;
      }

      @Override
      public byte[] getPayload(byte[] data, int offset) throws IOException {

        if (!payloadPending) {
          throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
        }

        if (Codec.DEBUG) {
          System.out.println("   getPayload payloadFP=" + payloadIn.getFilePointer() + " len=" + payloadLength);
        }

        final byte[] retArray;
        final int retOffset;
        if (data == null || data.length-offset < payloadLength) {
          // the array is too small to store the payload data,
          // so we allocate a new one
          retArray = new byte[payloadLength];
          retOffset = 0;
        } else {
          retArray = data;
          retOffset = offset;
        }

        payloadIn.readBytes(retArray, retOffset, payloadLength);
        payloadPending = false;
        return retArray;
      }
      
      @Override
      public boolean hasPayload() {
        return payloadPending && payloadLength > 0;
      }
    }
  }
}