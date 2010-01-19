package org.apache.lucene.search;

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
import org.apache.lucene.index.*;

/**
 * Position of a term in a document that takes into account the term offset within the phrase. 
 */
final class PhrasePositions {
  int doc;					  // current doc
  int position;					  // position in doc
  int count;					  // remaining pos in this doc
  int offset;					  // position in phrase
  final DocsEnum docs;				  // stream of docs
  PositionsEnum positions;                        // positions in current doc
  PhrasePositions next;	                          // used to make lists
  boolean repeats;       // there's other pp for same term (e.g. query="1st word 2nd word"~1) 

  PhrasePositions(DocsEnum docs, int o) {
    this.docs = docs;
    offset = o;
  }

  final boolean next() throws IOException {	  // increments to next doc
    doc = docs.nextDoc();
    if (doc == docs.NO_MORE_DOCS) {
      return false;
    }
    positions = docs.positions();
    return true;
  }

  final boolean skipTo(int target) throws IOException {
    doc = docs.advance(target);
    if (doc == docs.NO_MORE_DOCS) {
      return false;
    }
    return true;
  }


  final void firstPosition() throws IOException {
    count = docs.freq();				  // read first pos
    positions = docs.positions();
    if (positions == null) {
      throw new IllegalStateException("no positions are stored for this field (Field.omitTermFreqAndPositions was used)");
    }
    nextPosition();
  }

  /**
   * Go to next location of this term current document, and set 
   * <code>position</code> as <code>location - offset</code>, so that a 
   * matching exact phrase is easily identified when all PhrasePositions 
   * have exactly the same <code>position</code>.
   */
  final boolean nextPosition() throws IOException {
    if (count-- > 0) {				  // read subsequent pos's
      position = positions.next() - offset;
      return true;
    } else
      return false;
  }
}
