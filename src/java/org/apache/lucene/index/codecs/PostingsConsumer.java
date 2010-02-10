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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * NOTE: this API is experimental and will likely change
 */

public abstract class PostingsConsumer {

  // nocommit
  public String desc;

  // nocommit -- rename to startDoc?
  /** Adds a new doc in this term.  Return null if this
   *  consumer doesn't need to see the positions for this
   *  doc. */
  public abstract void addDoc(int docID, int termDocFreq) throws IOException;

  public static class PostingsMergeState {
    DocsEnum docsEnum;
    int[] docMap;
    int docBase;
  }

  /** Add a new position & payload.  A null payload means no
   *  payload; a non-null payload with zero length also
   *  means no payload.  Caller may reuse the {@link
   *  BytesRef} for the payload between calls (method must
   *  fully consume the payload). */
  public abstract void addPosition(int position, BytesRef payload) throws IOException;

  /** Called when we are done adding positions & payloads
   * for each doc */
  public abstract void finishDoc() throws IOException;

  /** Default merge impl: append documents, mapping around
   *  deletes */
  public int merge(MergeState mergeState, DocsEnum postings) throws IOException {

    int df = 0;

    if (mergeState.fieldInfo.omitTermFreqAndPositions) {
      while(true) {
        final int doc = postings.nextDoc();
        if (doc == DocsAndPositionsEnum.NO_MORE_DOCS) {
          break;
        }
        addDoc(doc, postings.freq());
        df++;
      }
    } else {
      final DocsAndPositionsEnum postingsEnum = (DocsAndPositionsEnum) postings;
      while(true) {
        final int doc = postingsEnum.nextDoc();
        if (doc == DocsAndPositionsEnum.NO_MORE_DOCS) {
          break;
        }
        final int freq = postingsEnum.freq();
        addDoc(doc, freq);
        for(int i=0;i<freq;i++) {
          final int position = postingsEnum.nextPosition();
          final int payloadLength = postingsEnum.getPayloadLength();
          final BytesRef payload;
          if (payloadLength > 0) {
            payload = postingsEnum.getPayload();
          } else {
            payload = null;
          }
          addPosition(position, payload);
        }
        df++;
      }
    }
    return df;
  }
}
