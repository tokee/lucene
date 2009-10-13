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

// TODO: split out an "iterator" api from the terms(String
// field) API?

// nocommit -- intended to be forward only?  eg no "reset"?

/** Access to fields and terms
 *
 * NOTE: this API is experimental and will likely change */

// TODO: someday expose public version of FieldInfos here
public abstract class Fields {

  // nocommit -- clarify if this is forwards only.  should
  // this be "skipTo"?
  // nocommit -- clarify: when this returns false, what is
  // its internal state?  eg if i call field() after getting
  // false back?
  /** Returns an iterator that will step through all fields
   *  names */
  public abstract FieldsEnum iterator() throws IOException;

  /** Get the {@link Terms} for this field */
  public abstract Terms terms(String field) throws IOException;
}

