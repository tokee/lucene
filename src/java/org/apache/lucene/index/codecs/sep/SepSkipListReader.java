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
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.MultiLevelSkipListReader;

/**
 * Implements the skip list reader for the default posting list format
 * that stores positions and payloads.
 *
 * @lucene.experimental
 */

// TODO: rewrite this as recursive classes?
class SepSkipListReader extends MultiLevelSkipListReader {
  private boolean currentFieldStoresPayloads;
  private IntIndexInput.Index freqIndex[];
  private IntIndexInput.Index docIndex[];
  private IntIndexInput.Index posIndex[];
  private long payloadPointer[];
  private int payloadLength[];

  private final IntIndexInput.Index lastFreqIndex;
  private final IntIndexInput.Index lastDocIndex;
  // nocommit -- make private again
  final IntIndexInput.Index lastPosIndex;
  
  private long lastPayloadPointer;
  private int lastPayloadLength;
                           
  SepSkipListReader(IndexInput skipStream,
                    IntIndexInput freqIn,
                    IntIndexInput docIn,
                    IntIndexInput posIn,
                    int maxSkipLevels,
                    int skipInterval)
    throws IOException {
    super(skipStream, maxSkipLevels, skipInterval);
    if (freqIn != null) {
      freqIndex = new IntIndexInput.Index[maxSkipLevels];
    }
    docIndex = new IntIndexInput.Index[maxSkipLevels];
    if (posIn != null) {
      posIndex = new IntIndexInput.Index[maxNumberOfSkipLevels];
    }
    for(int i=0;i<maxSkipLevels;i++) {
      if (freqIn != null) {
        freqIndex[i] = freqIn.index();
        if (Codec.DEBUG) {
          freqIndex[i].desc = "sslr.freq.level" + i;
        }
      }
      docIndex[i] = docIn.index();
      if (Codec.DEBUG) {
        docIndex[i].desc = "sslr.doc.level" + i;
      }
      if (posIn != null) {
        posIndex[i] = posIn.index();
        if (Codec.DEBUG) {
          posIndex[i].desc = "sslr.pos.level" + i;
        }
      }
    }
    payloadPointer = new long[maxSkipLevels];
    payloadLength = new int[maxSkipLevels];

    if (freqIn != null) {
      lastFreqIndex = freqIn.index();
    } else {
      lastFreqIndex = null;
    }
    lastDocIndex = docIn.index();
    if (posIn != null) {
      lastPosIndex = posIn.index();
    } else {
      lastPosIndex = null;
    }
  }
  
  void init(long skipPointer,
            IntIndexInput.Index docBaseIndex,
            IntIndexInput.Index freqBaseIndex,
            IntIndexInput.Index posBaseIndex,
            long payloadBasePointer,
            int df,
            boolean storesPayloads) {

    super.init(skipPointer, df);
    this.currentFieldStoresPayloads = storesPayloads;

    if (Codec.DEBUG) {
      System.out.println("ssr.init docBase=" + docBaseIndex + " freqBase=" + freqBaseIndex + " posBase=" + posBaseIndex + " payloadBase=" + payloadBasePointer + " df=" + df);
    }

    lastPayloadPointer = payloadBasePointer;

    for(int i=0;i<maxNumberOfSkipLevels;i++) {
      docIndex[i].set(docBaseIndex);
      if (freqIndex != null) {
        freqIndex[i].set(freqBaseIndex);
      }
      if (posBaseIndex != null) {
        posIndex[i].set(posBaseIndex);
      }
    }
    Arrays.fill(payloadPointer, payloadBasePointer);
    Arrays.fill(payloadLength, 0);
  }

  long getPayloadPointer() {
    return lastPayloadPointer;
  }
  
  /** Returns the payload length of the payload stored just before 
   * the doc to which the last call of {@link MultiLevelSkipListReader#skipTo(int)} 
   * has skipped.  */
  int getPayloadLength() {
    return lastPayloadLength;
  }
  
  @Override
  protected void seekChild(int level) throws IOException {
    super.seekChild(level);
    payloadPointer[level] = lastPayloadPointer;
    payloadLength[level] = lastPayloadLength;
  }
  
  @Override
  protected void setLastSkipData(int level) {
    super.setLastSkipData(level);

    lastPayloadPointer = payloadPointer[level];
    lastPayloadLength = payloadLength[level];
    if (freqIndex != null) {
      lastFreqIndex.set(freqIndex[level]);
    }
    lastDocIndex.set(docIndex[level]);
    if (lastPosIndex != null) {
      lastPosIndex.set(posIndex[level]);
    }

    if (level > 0) {
      if (freqIndex != null) {
        freqIndex[level-1].set(freqIndex[level]);
      }
      docIndex[level-1].set(docIndex[level]);
      if (posIndex != null) {
        posIndex[level-1].set(posIndex[level]);
      }
    }
  }

  IntIndexInput.Index getFreqIndex() {
    return lastFreqIndex;
  }

  IntIndexInput.Index getPosIndex() {
    return lastPosIndex;
  }

  IntIndexInput.Index getDocIndex() {
    return lastDocIndex;
  }

  @Override
  protected int readSkipData(int level, IndexInput skipStream) throws IOException {
    int delta;
    if (currentFieldStoresPayloads) {
      // the current field stores payloads.
      // if the doc delta is odd then we have
      // to read the current payload length
      // because it differs from the length of the
      // previous payload
      delta = skipStream.readVInt();
      if ((delta & 1) != 0) {
        payloadLength[level] = skipStream.readVInt();
      }
      delta >>>= 1;
    } else {
      delta = skipStream.readVInt();
    }
    //System.out.println("  delta=" + delta + " level=" +
    //level);
    if (freqIndex != null) {
      freqIndex[level].read(skipStream, false);
    }
    docIndex[level].read(skipStream, false);
    // nocommit -- make this explicit w/ omitTF, matching SepSkipListWriter
    if (posIndex != null) {
      posIndex[level].read(skipStream, false);
      payloadPointer[level] += skipStream.readVInt();
    }
    
    if (Codec.DEBUG) {
      System.out.println("ssr.readSkipData docDelta=" + delta + " curStoresPayloads=" + currentFieldStoresPayloads + " level=" + level + " freqIndex=" + (freqIndex==null?null:freqIndex[level]) + " docIndex=" + docIndex[level] + " posIndex=" + (posIndex==null? "null" : ""+posIndex[level]) + " payloadPointer=" + payloadPointer[level]);
    }
    return delta;
  }
}
