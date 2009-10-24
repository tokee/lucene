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

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

// nocommit -- circular, not clean
import org.apache.lucene.index.codecs.standard.StandardTermsDictReader.CacheEntry;

/** StandardTermsDictReader interacts with a single instance
 *  of this to manage creation of multiple docs enum
 *  instances.  It provides an IndexInput (termsIn) where
 *  this class may read any previously stored data that it
 *  had written in its corresponding StandarDocsConsumer at
 *  indexing time. */
public abstract class StandardDocsProducer {
  
  public abstract class Reader {
    
    public abstract void readTerm(int docFreq, boolean isIndexTerm) throws IOException;

    /** Returns a docs enum for the last term read */
    public abstract DocsEnum docs(Bits deletedDocs) throws IOException;
    
    // nocommit: fooling around with reusable
    public abstract CacheEntry captureState(CacheEntry reusableState) throws IOException;
    
    public abstract void setState(CacheEntry state, int docFreq) throws IOException;
    
    public boolean canCaptureState() {
      return false;
    }
  }

  public abstract void start(IndexInput termsIn) throws IOException;

  /** Returns a new private reader for stepping through
   *  terms, getting DocsEnum. */
  public abstract Reader reader(FieldInfo fieldInfo, IndexInput termsIn) throws IOException;

  public abstract void close() throws IOException;
}
