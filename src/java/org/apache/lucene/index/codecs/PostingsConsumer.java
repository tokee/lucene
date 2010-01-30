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
  /*
  public boolean setDesc(String desc) {
    this.desc = desc;
    return true;
  }
  */

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
  public int merge(MergeState mergeState, PostingsMergeState[] toMerge, int count) throws IOException {

    int df = 0;

    // Append docs in order:
    for(int i=0;i<count;i++) {
      final DocsEnum docsEnum = toMerge[i].docsEnum;
      final int[] docMap = toMerge[i].docMap;
      final int base = toMerge[i].docBase;

      final DocsAndPositionsEnum postingsEnum;

      if (!mergeState.omitTermFreqAndPositions) {
        postingsEnum = (DocsAndPositionsEnum) docsEnum;
      } else {
        postingsEnum = null;
      }

      while(true) {
        final int startDoc = docsEnum.nextDoc();
        if (startDoc == DocsAndPositionsEnum.NO_MORE_DOCS) {
          break;
        }
        df++;

        int doc;
        if (docMap != null) {
          // map around deletions
          doc = docMap[startDoc];
          assert doc != -1: "docs enum returned deleted docID " + startDoc + " freq=" + docsEnum.freq() + " df=" + df + " de=" + docsEnum;
        } else {
          doc = startDoc;
        }

        doc += base;                              // convert to merged space
        assert doc < mergeState.mergedDocCount: "doc=" + doc + " maxDoc=" + mergeState.mergedDocCount;

        final int freq = docsEnum.freq();

        addDoc(doc, freq);

        // nocommit -- omitTF should be "private", and this
        // code (and FreqProxTermsWriter) should instead
        // check if posConsumer is null?
        if (!mergeState.omitTermFreqAndPositions) {
          for(int j=0;j<freq;j++) {
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
          finishDoc();
        }
      }
    }

    return df;
  }
}
