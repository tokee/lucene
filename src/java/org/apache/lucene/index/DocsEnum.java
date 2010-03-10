package org.apache.lucene.index;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IntsRef;

/** Iterates through the documents, term freq and positions.
 *  NOTE: you must first call {@link #next}.
 *
 *  @lucene.experimental */
public abstract class DocsEnum extends DocIdSetIterator {

  private AttributeSource atts = null;

  // nocommit -- debugging
  public String desc;

  /** Returns term frequency in the current document.  Do
   *  not call this before {@link #next} is first called,
   *  nor after {@link #next} returns NO_MORE_DOCS. */
  public abstract int freq();
  
  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }

  // TODO: maybe add bulk read only docIDs (for eventual
  // match-only scoring)

  public static class BulkReadResult {
    public final IntsRef docs = new IntsRef();
    public final IntsRef freqs = new IntsRef();
    public int count;
  }

  protected BulkReadResult bulkResult;

  protected final void initBulkResult() {
    if (bulkResult == null) {
      bulkResult = new BulkReadResult();
      bulkResult.docs.ints = new int[64];
      bulkResult.freqs.ints = new int[64];
    }
  }
  
  /** Bulk read (docs and freqs).  After this is called,
   * {@link #doc} and {@link #freq} are undefined.  You must
   * refer to the count member of BulkResult to determine
   * how many docs were loaded (the IntsRef for docs and
   * freqs will not have their length set).  This method
   * will not return null.  The end has been reached when
   * .count is 0.
   * 
   *  <p>NOTE: the default impl simply delegates to {@link
   *  #nextDoc}, but subclasses may do this more
   *  efficiently. */
  // nocommit -- maybe pre-share the BulkReadResult.... hmm
  public BulkReadResult read() throws IOException {
    initBulkResult();
    int count = 0;
    final int[] docs = bulkResult.docs.ints;
    final int[] freqs = bulkResult.freqs.ints;
    while(count < docs.length) {
      final int doc = nextDoc();
      if (doc != NO_MORE_DOCS) {
        docs[count] = doc;
        freqs[count] = freq();
        count++;
      } else {
        break;
      }
    }
    bulkResult.count = count;
    return bulkResult;
  }
}
