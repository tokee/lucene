package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.PositionsEnum;

public abstract class PositionsConsumer {

  /** Add a new position & payload.  If payloadLength > 0
   *  you must read those bytes from the IndexInput.  NOTE:
   *  you must fully consume the byte[] payload, since
   *  caller is free to reuse it on subsequent calls. */
  public abstract void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) throws IOException;

  /** Called when we are done adding positions & payloads
   * for each doc */
  public abstract void finishDoc() throws IOException;

  private byte[] payloadBuffer;

  /** Default merge impl, just copies positions & payloads
   *  from the input. */
  public void merge(MergeState mergeState, PositionsEnum positions, int freq) throws IOException {
    for(int i=0;i<freq;i++) {
      final int position = positions.next();
      final int payloadLength = positions.getPayloadLength();
      if (payloadLength > 0) {
        if (payloadBuffer == null || payloadBuffer.length < payloadLength) {
          payloadBuffer = new byte[payloadLength];
        }
        positions.getPayload(payloadBuffer, 0);
      }
      addPosition(position, payloadBuffer, 0, payloadLength);
    }
    finishDoc();
  }
}
