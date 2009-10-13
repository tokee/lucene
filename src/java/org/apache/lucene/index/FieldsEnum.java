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

/** Enumerates indexed fields.
 *
 * NOTE: this API is experimental and will likely change */

public abstract class FieldsEnum extends AttributeSource {

  // nocommit -- do we need seek?

  /** Increments the enumeration to the next field.
   *  Returns null when there are no more fields.*/
  public abstract String next() throws IOException;

  /** Get TermsEnum for the current field.  You should not
   *  call {@link #next()} until you're done using this
   *  TermsEnum. */
  public abstract TermsEnum terms() throws IOException;
}

