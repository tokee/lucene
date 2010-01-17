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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * NOTE: this API is experimental and will likely change
 */

public abstract class Terms {

  /** Returns an iterator that will step through all terms */
  public abstract TermsEnum iterator() throws IOException;
  
  /** Return the BytesRef Comparator used to sort terms
   *  provided by the iterator.  NOTE: this may return null
   *  if there are no terms.  This method may be invoked
   *  many times; it's best to cache a single instance &
   *  reuse it. */
  public abstract BytesRef.Comparator getComparator() throws IOException;

  /** Returns the docFreq of the specified term text. */
  public int docFreq(BytesRef text) throws IOException {
    // nocommit -- make thread private cache so we share
    // single enum
    // NOTE: subclasses may have more efficient impl
    final TermsEnum terms = iterator();
    if (terms.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return terms.docFreq();
    } else {
      return 0;
    }
  }

  /** Get DocsEnum for the specified term. */
  public DocsEnum docs(Bits skipDocs, BytesRef text) throws IOException {
    // NOTE: subclasses may have more efficient impl
    final TermsEnum terms = iterator();
    if (terms.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return terms.docs(skipDocs);
    } else {
      return null;
    }
  }

  public long getUniqueTermCount() throws IOException {
    throw new UnsupportedOperationException("this reader does not implement getUniqueTermCount()");
  }
}
