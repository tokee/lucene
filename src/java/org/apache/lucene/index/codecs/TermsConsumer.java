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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.MultiDocsAndPositionsEnum;

import org.apache.lucene.util.BytesRef;

/**
 * @lucene.experimental
 */

public abstract class TermsConsumer {

  /** Starts a new term in this field. */
  public abstract PostingsConsumer startTerm(BytesRef text) throws IOException;

  /** Finishes the current term */
  public abstract void finishTerm(BytesRef text, int numDocs) throws IOException;

  /** Called when we are done adding terms to this field */
  public abstract void finish() throws IOException;

  /** Return the BytesRef Comparator used to sort terms
   *  before feeding to this API. */
  public abstract BytesRef.Comparator getComparator() throws IOException;

  /** Default merge impl */
  private MappingMultiDocsEnum docsEnum = null;
  private MappingMultiDocsAndPositionsEnum postingsEnum = null;

  public void merge(MergeState mergeState, TermsEnum termsEnum) throws IOException {

    BytesRef term;

    if (mergeState.fieldInfo.omitTermFreqAndPositions) {
      if (docsEnum == null) {
        docsEnum = new MappingMultiDocsEnum();
      }
      docsEnum.setMergeState(mergeState);

      MultiDocsEnum docsEnumIn = null;

      while((term = termsEnum.next()) != null) {
        MultiDocsEnum docsEnumIn2 = (MultiDocsEnum) termsEnum.docs(mergeState.multiDeletedDocs, docsEnumIn);
        if (docsEnumIn2 != null) {
          docsEnumIn = docsEnumIn2;
          docsEnum.reset(docsEnumIn);
          final PostingsConsumer postingsConsumer = startTerm(term);
          final int numDocs = postingsConsumer.merge(mergeState, docsEnum);
          finishTerm(term, numDocs);
        }
      }
    } else {
      if (postingsEnum == null) {
        postingsEnum = new MappingMultiDocsAndPositionsEnum();
      }
      postingsEnum.setMergeState(mergeState);
      MultiDocsAndPositionsEnum postingsEnumIn = null;
      while((term = termsEnum.next()) != null) {
        MultiDocsAndPositionsEnum postingsEnumIn2 = (MultiDocsAndPositionsEnum) termsEnum.docsAndPositions(mergeState.multiDeletedDocs, postingsEnumIn);
        if (postingsEnumIn2 != null) {
          postingsEnumIn = postingsEnumIn2;
          postingsEnum.reset(postingsEnumIn);
          final PostingsConsumer postingsConsumer = startTerm(term);
          final int numDocs = postingsConsumer.merge(mergeState, postingsEnum);
          finishTerm(term, numDocs);
        }
      }
    }
  }
}
