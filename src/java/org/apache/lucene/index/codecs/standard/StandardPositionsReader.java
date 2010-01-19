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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

public class StandardPositionsReader extends StandardPositionsProducer {
  
  IndexInput proxIn;
  IndexInput termsIn;

  public StandardPositionsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize) throws IOException {
    assert segmentInfo.getHasProx();
    String file = IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.PROX_EXTENSION);
    if(dir.fileExists(file)) {
      proxIn = dir.openInput(file, readBufferSize);
    }
  }
    

  @Override
  public void start(IndexInput termsIn) throws IOException {
    this.termsIn = termsIn;

    Codec.checkHeader(termsIn, StandardPositionsWriter.CODEC, StandardPositionsWriter.VERSION_START);
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, Collection<String> files) throws IOException {
    if (segmentInfo.getHasProx()) {
      String file = IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.PROX_EXTENSION);
      if (dir.fileExists(file))
        files.add(IndexFileNames.segmentFileName(segmentInfo.name, StandardCodec.PROX_EXTENSION));
    }
  }

  @Override
  public Reader reader(FieldInfo fieldInfo, IndexInput termsIn) {
    return new TermsDictReader(termsIn, fieldInfo);
  }

  @Override
  public void close() throws IOException {
    if (proxIn != null) {
      proxIn.close();
    }
  }

  class TermsDictReader extends Reader {

    final IndexInput termsIn;
    final FieldInfo fieldInfo;
    long proxOffset;

    TermsDictReader(IndexInput termsIn, FieldInfo fieldInfo) {
      this.termsIn = termsIn;
      this.fieldInfo = fieldInfo;
    }

    @Override
    public void readTerm(int docFreq, boolean isIndexTerm) throws IOException {
      // mxx
      if (Codec.DEBUG) {
        System.out.println("    pr.readterm termsInPointer=" + termsIn.getFilePointer() + " isIndex=" + isIndexTerm);
      }

      if (isIndexTerm) {
        proxOffset = termsIn.readVLong();
      } else {
        proxOffset += termsIn.readVLong();
      }

      // mxx
      if (Codec.DEBUG) {
        System.out.println("      proxOffset=" + proxOffset);
      }

      if (positions != null) {
        positions.seekPending = true;
        positions.skipOffset = proxOffset;
        positions.skipPosCount = 0;
      }
    }

    SegmentPositionsEnum positions;

    @Override
    public PositionsEnum positions() throws IOException {

      if (positions == null) {
        // Lazy init
        positions = new SegmentPositionsEnum();
      }

      return positions;
    }

    class SegmentPositionsEnum extends PositionsEnum {

      // nocommit
      String desc;

      final IndexInput proxIn;

      final boolean storePayloads;

      boolean seekPending;                        // True if we must seek before reading next position
      boolean payloadPending;                     // True if we must skip payload beore reading next position

      long skipOffset;
      int skipPosCount;

      int position;
      int payloadLength;

      SegmentPositionsEnum() {
        if (Codec.DEBUG) {
          System.out.println("new pos enum");
        }
        proxIn = (IndexInput) StandardPositionsReader.this.proxIn.clone();
        storePayloads = fieldInfo.storePayloads;
      }

      void skip(long proxOffset, int lastPayloadLength, int numPositions) {
        skipOffset = proxOffset;
        payloadLength = lastPayloadLength;
        assert payloadLength >= 0 || payloadLength == -1;
        skipPosCount = numPositions;
        seekPending = true;
        payloadPending = false;
        if (Codec.DEBUG) {
          System.out.println("pr [" + desc + "] skip fp= " + proxOffset + " numPositions=" + numPositions);
        }
      }

      void skip(int numPositions) {
        skipPosCount += numPositions;
        if (Codec.DEBUG) {
          System.out.println("pr [" + desc + "] skip " + numPositions + " positions; now " + skipPosCount);
        }
      }

      void catchUp(int currentCount) throws IOException { 
        if (Codec.DEBUG) {
          System.out.println("  pos catchup: seekPending=" + seekPending + " skipOffset=" + skipOffset + " skipPosCount " + skipPosCount + " vs currentCount " + currentCount + " payloadLen=" + payloadLength);
        }

        if (seekPending) {
          proxIn.seek(skipOffset);
          seekPending = false;
        }

        while(skipPosCount > currentCount) {
          next();
        }
        if (Codec.DEBUG) {
          System.out.println("  pos catchup done");
        }
        positions.init();
      }

      void init() {
        if (Codec.DEBUG) {
          System.out.println("  pos init");
        }
        position = 0;
      }

      @Override
      public int next() throws IOException {

        if (Codec.DEBUG) {
          System.out.println("    pr.next [" + desc + "]: fp=" + proxIn.getFilePointer() + " return pos=" + position);
        }

        if (storePayloads) {

          if (payloadPending && payloadLength > 0) {
            if (Codec.DEBUG) {
              System.out.println("      payload pending: skip " + payloadLength + " bytes");
            }
            proxIn.seek(proxIn.getFilePointer()+payloadLength);
          }

          final int code = proxIn.readVInt();
          if ((code & 1) != 0) {
            // Payload length has changed
            payloadLength = proxIn.readVInt();
            assert payloadLength >= 0;
            if (Codec.DEBUG) {
              System.out.println("      new payloadLen=" + payloadLength);
            }
          }
          assert payloadLength != -1;
          
          payloadPending = true;
          position += code >>> 1;
        } else
          position += proxIn.readVInt();

        skipPosCount--;

        // NOTE: the old API actually allowed this...
        assert skipPosCount >= 0: "next() was called too many times (more than FormatPostingsDocsEnum.freq() times) skipPosCount=" + skipPosCount;

        if (Codec.DEBUG) {
          System.out.println("   proxFP=" + proxIn.getFilePointer() + " return pos=" + position);
        }
        return position;
      }

      @Override
      public int getPayloadLength() {
        return payloadLength;
      }

      private BytesRef payload;

      @Override
      public BytesRef getPayload() throws IOException {
        if (!payloadPending) {
          throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
        }
        if (payload == null) {
          payload = new BytesRef();
          payload.bytes = new byte[payloadLength];
        } else if (payloadLength > payload.bytes.length) {
          payload.grow(payloadLength);
        }
        proxIn.readBytes(payload.bytes, 0, payloadLength);
        payload.length = payloadLength;
        payloadPending = false;

        return payload;
      }
      
      @Override
      public boolean hasPayload() {
        return payloadPending && payloadLength > 0;
      }
    }
  }
}
