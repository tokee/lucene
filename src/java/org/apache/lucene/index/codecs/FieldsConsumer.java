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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/** Abstract API that consumes terms, doc, freq, prox and
 *  payloads postings.  Concrete implementations of this
 *  actually do "something" with the postings (write it into
 *  the index in a specific format).
 *
 * NOTE: this API is experimental and will likely change
 */
public abstract class FieldsConsumer {

  /** Add a new field */
  public abstract TermsConsumer addField(FieldInfo field) throws IOException;

  /** Called when we are done adding everything. */
  public abstract void close() throws IOException;

  private final static class FieldMergeState {
    String current;
    FieldsEnum fieldsEnum;
    int readerIndex;
  }

  // Used for merge-sorting by field
  private final static class MergeQueue extends PriorityQueue<FieldMergeState> {
    public MergeQueue(int size) {
      initialize(size);
    }

    @Override
    protected final boolean lessThan(FieldMergeState a, FieldMergeState b) {
      final int cmp = a.current.compareTo(b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        // nocommit -- technically not required to break
        // ties, since the terms merging will do so?
        return a.readerIndex < b.readerIndex;
      }
    }
  }

  public void merge(MergeState mergeState, Fields[] fields) throws IOException {

    MergeQueue queue = new MergeQueue(fields.length);

    for(int i=0;i<fields.length;i++) {
      FieldsEnum fieldsEnum = fields[i].iterator();
      String field = fieldsEnum.next();
      if (field != null) {
        FieldMergeState state = new FieldMergeState();
        state.current = field;
        state.fieldsEnum = fieldsEnum;
        state.readerIndex = i;
        queue.add(state);
      } else {
        // no fields at all -- nothing to do
      }
    }

    final FieldMergeState[] pending = new FieldMergeState[mergeState.readerCount];
    final TermsConsumer.TermMergeState[] match = new TermsConsumer.TermMergeState[mergeState.readerCount];
    for(int i=0;i<mergeState.readerCount;i++) {
      match[i] = new TermsConsumer.TermMergeState();
    }

    // Merge sort by field name, calling terms.merge on all
    // fields sharing same field name:
    while(queue.size() != 0) {

      int matchCount = 0;
      int pendingCount = 0;

      while(true) {
        FieldMergeState state = pending[pendingCount++] = queue.pop();
        TermsEnum termsEnum = state.fieldsEnum.terms();
        if (termsEnum != null) {
          match[matchCount].termsEnum = termsEnum;
          match[matchCount].readerIndex = state.readerIndex;
          matchCount++;
        }
        FieldMergeState top = queue.top();
        if (top == null || top.current != pending[0].current) {
          break;
        }
      }

      if (matchCount > 0) {
        // Merge one field
        final String field = pending[0].current;
        mergeState.fieldInfo = mergeState.fieldInfos.fieldInfo(field);
        mergeState.omitTermFreqAndPositions = mergeState.fieldInfo.omitTermFreqAndPositions;
        final TermsConsumer termsConsumer = addField(mergeState.fieldInfo);
        termsConsumer.merge(mergeState, match, matchCount);
      }

      // Put fields back into queue
      for(int i=0;i<pendingCount;i++) {
        FieldMergeState state = pending[i];
        
        state.current = state.fieldsEnum.next();
        if (state.current != null) {
          // More fields to merge
          queue.add(state);
        } else {
          // Done
        }
      }
    }

  }
}
