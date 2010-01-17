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
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.BytesRef;

/**
 * NOTE: this API is experimental and will likely change
 */

public abstract class TermsConsumer {

  /** Starts a new term in this field. */
  public abstract DocsConsumer startTerm(BytesRef text) throws IOException;

  /** Finishes the current term */
  public abstract void finishTerm(BytesRef text, int numDocs) throws IOException;

  /** Called when we are done adding terms to this field */
  public abstract void finish() throws IOException;

  /** Return the BytesRef Comparator used to sort terms
   *  before feeding to this API. */
  public abstract BytesRef.Comparator getComparator() throws IOException;

  // For default merge impl
  public static class TermMergeState {
    BytesRef current;
    TermsEnum termsEnum;
    int readerIndex;
  }

  private final static class MergeQueue extends PriorityQueue<TermMergeState> {

    final BytesRef.Comparator termComp;

    public MergeQueue(int size, BytesRef.Comparator termComp) {
      initialize(size);
      this.termComp = termComp;
    }

    @Override
    protected final boolean lessThan(TermMergeState a, TermMergeState b) {
      final int cmp = termComp.compare(a.current, b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return a.readerIndex < b.readerIndex;
      }
    }
  }

  private MergeQueue queue;
  private DocsConsumer.DocsMergeState[] match;
  private TermMergeState[] pending;

  /** Default merge impl */
  public void merge(MergeState mergeState, TermMergeState[] termsStates, int count) throws IOException {

    final BytesRef.Comparator termComp = getComparator();

    //System.out.println("merge terms field=" + mergeState.fieldInfo.name + " comp=" + termComp);

    if (queue == null) {
      queue = new MergeQueue(mergeState.readerCount, termComp);
      match = new DocsConsumer.DocsMergeState[mergeState.readerCount];
      for(int i=0;i<mergeState.readerCount;i++) {
        match[i] = new DocsConsumer.DocsMergeState();
      }
      pending = new TermMergeState[mergeState.readerCount];
    } else if (!queue.termComp.equals(termComp)) {
      queue = new MergeQueue(mergeState.readerCount, termComp);
    }

    // Init queue
    for(int i=0;i<count;i++) {
      TermMergeState state = termsStates[i];
      state.current = state.termsEnum.next();
      if (state.current != null) {
        queue.add(state);
      } else {
        // no terms at all in this field
      }
    }

    while(queue.size() != 0) {

      int matchCount = 0;
      int pendingCount = 0;

      while(true) {
        TermMergeState state = pending[pendingCount++] = queue.pop();
        
        DocsEnum docsEnum = state.termsEnum.docs(mergeState.readers.get(state.readerIndex).getDeletedDocs());
        if (docsEnum != null) {
          match[matchCount].docsEnum = docsEnum;
          match[matchCount].docMap = mergeState.docMaps[state.readerIndex];
          match[matchCount].docBase = mergeState.docBase[state.readerIndex];
          matchCount++;
        }
        TermMergeState top = queue.top();
        if (top == null || !top.current.bytesEquals(pending[0].current)) {
          break;
        }
      }

      if (matchCount > 0) {
        // Merge one term
        final BytesRef term = pending[0].current;
        //System.out.println("  merge term=" + term);
        final DocsConsumer docsConsumer = startTerm(term);
        final int numDocs = docsConsumer.merge(mergeState, match, matchCount);
        finishTerm(term, numDocs);
      }

      // Put terms back into queue
      for(int i=0;i<pendingCount;i++) {
        TermMergeState state = pending[i];
        
        state.current = state.termsEnum.next();
        if (state.current != null) {
          // More terms to merge
          queue.add(state);
        } else {
          // Done
        }
      }
    }
  }
}
