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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.TermsEnum;

/**
 * Subclass of {@code FilteredTermsEnum} that is always empty.
 * <p>
 * This can be used by {@link MultiTermQuery}s (if no terms can ever match the query),
 * but want to preserve MultiTermQuery semantics such as
 * {@link MultiTermQuery#rewriteMethod}.
 */
public final class EmptyTermsEnum extends FilteredTermsEnum {
  
  /**
   * Creates a new <code>EmptyTermsEnum</code>.
   */
  public EmptyTermsEnum() {
    super((TermsEnum) null);
  }

  @Override
  /** Always returns {@link AcceptStatus#END}. */
  protected AcceptStatus accept(BytesRef term) {
    return AcceptStatus.END;
  }

  /** Always returns {@link SeekStatus#END}. */
  @Override
  public SeekStatus seek(BytesRef term) {
    return SeekStatus.END;
  }

  /** Always returns {@link SeekStatus#END}. */
  @Override
  public SeekStatus seek(long ord) {
    return SeekStatus.END;
  }

}
