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

import java.util.HashSet;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.Codecs;

/**
 * This class is not meant for public usage; it's only
 * public in order to expose access across packages.  It's
 * used internally when updating the index.
 * @lucene.experimental
 */
public class SegmentWriteState {
  // nocommit -- not clean that this is here; sometimes we
  // write a newly flushed segment; other times a merged
  // segment (and this is null):
  DocumentsWriter docWriter;
  public Directory directory;
  public String segmentName;
  public FieldInfos fieldInfos;
  String docStoreSegmentName;
  public int numDocs;
  int numDocsInStore;
  public Collection<String> flushedFiles;

  // Actual codec used
  Codec codec;

  /** Expert: The fraction of terms in the "dictionary" which should be stored
   * in RAM.  Smaller values use more memory, but make searching slightly
   * faster, while larger values use less memory and make searching slightly
   * slower.  Searching is typically not dominated by dictionary lookup, so
   * tweaking this is rarely useful.*/
  public int termIndexInterval;

  /** Expert: The fraction of {@link TermDocs} entries stored in skip tables,
   * used to accelerate {@link TermDocs#skipTo(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  public int skipInterval = 16;
  
  /** Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  public int maxSkipLevels = 10;

  public SegmentWriteState(DocumentsWriter docWriter, Directory directory, String segmentName, FieldInfos fieldInfos,
                           String docStoreSegmentName, int numDocs,
                           int numDocsInStore, int termIndexInterval,
                           Codecs codecs) {
    this.docWriter = docWriter;
    this.directory = directory;
    this.segmentName = segmentName;
    this.fieldInfos = fieldInfos;
    this.docStoreSegmentName = docStoreSegmentName;
    this.numDocs = numDocs;
    this.numDocsInStore = numDocsInStore;
    this.termIndexInterval = termIndexInterval;
    this.codec = codecs.getWriter(this);
    flushedFiles = new HashSet<String>();
  }
}
