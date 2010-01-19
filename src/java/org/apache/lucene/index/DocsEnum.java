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

/** On obtaining a DocsEnum, you must first call next() */

public abstract class DocsEnum extends DocIdSetIterator {

  private AttributeSource atts = null;

  // nocommit
  public String desc;

  public abstract int freq();
  
  /**
   * Returns the related attributes.
   */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
  
  // nocommit -- state in API that doc/freq are undefined
  // (defined?) after this?
  // nocommit -- fix this API so that intblock codecs are
  // able to return their own int arrays, to save a copy...  IntsRef?
  /** Bulk read: returns number of docs read.
   * 
   *  <p>NOTE: the default impl simply delegates to {@link
   *  #nextDoc}, but subclasses may do this more
   *  efficiently. */
  public int read(int[] docs, int[] freqs) throws IOException {
    int count = 0;
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
    return count;
  }

  /** Don't call next() or skipTo() or read() until you're
   *  done consuming the positions.  NOTE: this method may
   *  return null, if the index contains no positional
   *  information for this document.  The standard codec
   *  (default) does this today when the field was indexed
   *  with {@link Field#setOmitTermFreqAndPositions}. */
  public abstract PositionsEnum positions() throws IOException;
}
