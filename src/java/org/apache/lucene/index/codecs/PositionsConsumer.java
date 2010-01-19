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
import org.apache.lucene.util.BytesRef;

public abstract class PositionsConsumer {

  /** Add a new position & payload.  A null payload means no
   *  payload; a non-null payload with zero length also
   *  means no payload.  Caller may reuse the {@link
   *  BytesRef} for the payload between calls (method must
   *  fully consume the payload). */
  public abstract void add(int position, BytesRef payload) throws IOException;

  /** Called when we are done adding positions & payloads
   * for each doc */
  public abstract void finishDoc() throws IOException;

  private BytesRef payload;

  /** Default merge impl, just copies positions & payloads
   *  from the input. */
  public void merge(MergeState mergeState, PositionsEnum positions, int freq) throws IOException {
    for(int i=0;i<freq;i++) {
      final int position = positions.next();
      final int payloadLength = positions.getPayloadLength();

      final BytesRef payload;
      if (payloadLength > 0) {
        payload = positions.getPayload();
      } else {
        payload = null;
      }
      add(position, payload);
    }
    finishDoc();
  }
}
