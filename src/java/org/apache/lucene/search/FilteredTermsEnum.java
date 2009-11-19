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
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.Bits;

/**
 * Abstract class for enumerating a subset of all terms. 
 *
 * <p>On creation, the enumerator must already be positioned
 * to the first term.</p>
 * 
 * <p>Term enumerations are always ordered by
 * Term.compareTo().  Each term in the enumeration is
 * greater than all that precede it.</p>
*/
public abstract class FilteredTermsEnum extends TermsEnum {

  protected static enum AcceptStatus {YES, NO, END};
    
  /** the delegate enum - to set this member use {@link #setEnum} */
  protected TermsEnum actualEnum;
    
  /** Return true if term is acceptd */
  protected abstract AcceptStatus accept(TermRef term);
    
  /** Equality measure on the term */
  public abstract float difference();

  public abstract String field();

  /** Only called once, right after construction, to check
   *  whether there are no matching terms */
  public abstract boolean empty();

  /**
   * use this method to set the actual TermsEnum (e.g. in ctor),
   * it will be automatically positioned on the first
   * accepted term, and returns the term found or null if
   * there is no matching term.
   */
  protected TermRef setEnum(TermsEnum actualEnum, TermRef term) throws IOException {
    this.actualEnum = actualEnum;

    // Find the first term that matches
    if (term != null) {
      SeekStatus status = actualEnum.seek(term);
      if (status == SeekStatus.END) {
        return null;
      } else {
        AcceptStatus s = accept(actualEnum.term());
        if (s == AcceptStatus.NO) {
          return next();
        } else if (s == AcceptStatus.END) {
          return null;
        } else {
          return actualEnum.term();
        }
      }
    } else {
      return next();
    }
  }

  @Override
  public TermRef term() throws IOException {
    if(actualEnum == null) {
      return null;
    }
    return actualEnum.term();
  }
    
  /** 
   * Returns the docFreq of the current Term in the enumeration.
   * Returns -1 if no Term matches or all terms have been enumerated.
   */
  @Override
  public int docFreq() {
    assert actualEnum != null;
    return actualEnum.docFreq();
  }
    
  /** Increments the enumeration to the next element.
   * Non-null if one exists, or null if it's the end. */
  @Override
  public TermRef next() throws IOException {
    assert actualEnum != null;
    while (true) {
      TermRef term = actualEnum.next();
      if (term != null) {
        AcceptStatus s = accept(term);
        if (s == AcceptStatus.YES) {
          return term;
        } else if (s == AcceptStatus.END) {
          // end
          return null;
        }
      } else {
        // end
        return null;
      }
    }
  }

  @Override
  public SeekStatus seek(TermRef term) throws IOException {
    return finishSeek(actualEnum.seek(term));
  }

  @Override
  public SeekStatus seek(long ord) throws IOException {
    return finishSeek(actualEnum.seek(ord));
  }

  private SeekStatus finishSeek(SeekStatus status) throws IOException {
    if (status != SeekStatus.END) {
      TermRef term = actualEnum.term();
      final AcceptStatus s = accept(term);
      if (s == AcceptStatus.NO) {
        term = next();
        if (term == null) {
          return SeekStatus.END;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      } else if (s == AcceptStatus.END) {
        return SeekStatus.END;
      } else {
        return status;
      }
    } else {
      return status;
    }
  }

  @Override
  public long ord() throws IOException {
    return actualEnum.ord();
  }

  @Override
  public DocsEnum docs(Bits bits) throws IOException {
    return actualEnum.docs(bits);
  }
}
