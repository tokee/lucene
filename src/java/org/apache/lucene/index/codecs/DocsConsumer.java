package org.apache.lucene.index.codecs;

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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfo;

/**
 * NOTE: this API is experimental and will likely change
 */

// nocommit -- name this "StandardDocsConsumer"?  eg the
// RAMCodec doesn't need most of these methods...
public abstract class DocsConsumer {

  // nocommit
  public String desc;
  /*
  public boolean setDesc(String desc) {
    this.desc = desc;
    return true;
  }
  */

  public abstract void start(IndexOutput termsOut) throws IOException;

  public abstract void startTerm() throws IOException;

  /** Adds a new doc in this term.  Return null if this
   *  consumer doesn't need to see the positions for this
   *  doc. */
  public abstract PositionsConsumer addDoc(int docID, int termDocFreq) throws IOException;

  /** Finishes the current term */
  public abstract void finishTerm(int numDocs, boolean isIndexTerm) throws IOException;

  public abstract void setField(FieldInfo fieldInfo);

  public abstract void close() throws IOException;
}
