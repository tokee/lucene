package org.apache.lucene.search.spans;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.PositionsEnum;

import java.io.IOException;
import java.util.Collections;
import java.util.Collection;

/**
 * Expert:
 * Public for extension only
 */
public class TermSpans extends Spans {
  protected final DocsEnum docs;
  protected PositionsEnum positions;
  protected final Term term;
  protected int doc;
  protected int freq;
  protected int count;
  protected int position;

  public TermSpans(DocsEnum docs, Term term) throws IOException {
    this.docs = docs;
    this.term = term;
    doc = -1;
  }

  public boolean next() throws IOException {
    if (count == freq) {
      doc = docs.next();
      if (doc == DocsEnum.NO_MORE_DOCS) {
        return false;
      }
      freq = docs.freq();
      positions = docs.positions();
      count = 0;
    }
    position = positions.next();
    count++;
    return true;
  }

  public boolean skipTo(int target) throws IOException {
    doc = docs.advance(target);
    if (doc == DocsEnum.NO_MORE_DOCS) {
      return false;
    }

    freq = docs.freq();
    count = 0;
    positions = docs.positions();

    position = positions.next();
    count++;

    return true;
  }

  public int doc() {
    return doc;
  }

  public int start() {
    return position;
  }

  public int end() {
    return position + 1;
  }

  // TODO: Remove warning after API has been finalized
  public Collection<byte[]> getPayload() throws IOException {
    byte [] bytes = new byte[positions.getPayloadLength()]; 
    bytes = positions.getPayload(bytes, 0);
    return Collections.singletonList(bytes);
  }

  // TODO: Remove warning after API has been finalized
 public boolean isPayloadAvailable() {
    return positions.hasPayload();
  }

  public String toString() {
    return "spans(" + term.toString() + ")@" +
            (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
  }

  public PositionsEnum getPositions() {
    return positions;
  }
}
