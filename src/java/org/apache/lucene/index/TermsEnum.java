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

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Iterator to seek ({@link #seek}) or step through ({@link
 * #next} terms, obtain frequency information ({@link
 * #docFreq}), and obtain a {@link DocsEnum} for the current
 * term ({@link #docs)}.
 * 
 * <p>Term enumerations are always ordered by
 * {@link #getTermComparator}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 *
 * <p>On obtaining a TermsEnum, you must first call
 * {@link #next} or {@link #seek}.
 *
 * @lucene.experimental */
public abstract class TermsEnum {

  private AttributeSource atts = null;

  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
  
  /** Represents returned result from {@link TermsEnum.seek}.
   *  If status is FOUND, then the precise term was found.
   *  If status is NOT_FOUND, then a different term was
   *  found.  If the status is END, the end of the iteration
   *  was hit. */
  public static enum SeekStatus {END, FOUND, NOT_FOUND};

  /** Seeks to the specified term.  Returns SeekResult to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be befor or after the current term. */
  public abstract SeekStatus seek(BytesRef text) throws IOException;

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be befor or after the current ord.  See {@link
   *  #seek(BytesRef). */
  public abstract SeekStatus seek(long ord) throws IOException;
  
  /** Increments the enumeration to the next element.
   *  Returns the resulting term, or null if the end was
   *  hit.  The returned BytesRef may be re-used across calls
   *  to next. */
  public abstract BytesRef next() throws IOException;

  /** Returns current term. Do not call this before calling
   *  next() for the first time, after next() returns null
   *  or seek returns {@link SeekStatus#END}.*/
  public abstract BytesRef term() throws IOException;

  /** Returns ordinal position for current term.  This is an
   *  optional method (the codec may throw {@link
   *  UnsupportedOperationException}).  Do not call this
   *  before calling next() for the first time, after next()
   *  returns null or seek returns {@link
   *  SeekStatus#END}. */
  public abstract long ord() throws IOException;

  /** Returns the number of documents containing the current
   *  term.  Do not call this before calling next() for the
   *  first time, after next() returns null or seek returns
   *  {@link SeekStatus#END}.*/
  public abstract int docFreq();

  // nocommit -- clarify if this may return null
  // nocommit -- maybe require up front boolean doPositions?
  // nocommit -- or maybe make a separate positions(...) method?
  /** Get {@link DocsEnum} for the current term.  Do not
   *  call this before calling next() for the first time,
   *  after next() returns null or seek returns {@link
   *  SeekStatus#END}.
   *  
   * @param skipDocs set bits are documents that should not
   * be returned
   * @param reuse pass a prior DocsEnum for possible reuse */
  public abstract DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException;

  public abstract DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException;

  /** Return the {@link BytesRef} Comparator used to sort
   *  terms provided by the iterator.  NOTE: this may return
   *  null if there are no terms.  Callers may invoke this
   *  method many times, so it's best to cache a single
   *  instance & reuse it. */
  public abstract BytesRef.Comparator getComparator() throws IOException;
}
