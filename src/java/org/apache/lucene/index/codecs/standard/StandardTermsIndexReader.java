package org.apache.lucene.index.codecs.standard;

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
import org.apache.lucene.index.TermRef;

import java.io.IOException;
import java.util.Collection;


// TODO
//   - allow for non-regular index intervals?  eg with a
//     long string of rare terms, you don't need such
//     frequent indexing

/**
 * TermsDictReader interacts with an instance of this class
 * to manage its terms index.  The writer must accept
 * indexed terms (many pairs of CharSequence text + long
 * fileOffset), and then this reader must be able to
 * retrieve the nearest index term to a provided term
 * text. */

public abstract class StandardTermsIndexReader {

  static class TermsIndexResult {
    int position;
    final TermRef term = new TermRef();
    long offset;
  };

  public abstract class FieldReader {
    /** Returns position of "largest" index term that's <=
     *  text.  Returned TermsIndexResult may be reused
     *  across calls.  This resets internal state, and
     *  expects that you'll then scan the file and
     *  sequentially call isIndexTerm for each term
     *  encountered. */
    public abstract void getIndexOffset(TermRef term, TermsIndexResult result) throws IOException;

    public abstract void getIndexOffset(long ord, TermsIndexResult result) throws IOException;

    /** Call this sequentially for each term encoutered,
     *  after calling {@link #getIndexOffset}. */
    public abstract boolean isIndexTerm(int position, int docFreq) throws IOException;
  }

  public abstract FieldReader getField(FieldInfo fieldInfo);

  public abstract void loadTermsIndex(int indexDivisor) throws IOException;

  public abstract void close() throws IOException;

  public abstract void getExtensions(Collection<String> extensions);
}