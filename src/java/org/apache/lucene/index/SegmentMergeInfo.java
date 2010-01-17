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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.util.BytesRef;

final class SegmentMergeInfo {
  int base;
  int ord;  // the position of the segment in a MultiReader
  final FieldsEnum fields;
  TermsEnum terms;
  String field;
  BytesRef term;

  IndexReader reader;
  int delCount;
  //private TermPositions postings;  // use getPositions()
  private int[] docMap;  // use getDocMap()

  // nocommit
  private String segment;

  SegmentMergeInfo(int b, IndexReader r)
    throws IOException {
    base = b;
    reader = r;
    fields = r.fields().iterator();
    // nocommit
    if (Codec.DEBUG) {
      if (r instanceof SegmentReader) {
        segment = ((SegmentReader) r).core.segment;
      }
      System.out.println("smi create seg=" + segment);
    }
  }

  // maps around deleted docs
  int[] getDocMap() {
    if (docMap == null) {
      delCount = 0;
      // build array which maps document numbers around deletions 
      if (reader.hasDeletions()) {
        int maxDoc = reader.maxDoc();
        docMap = new int[maxDoc];
        int j = 0;
        for (int i = 0; i < maxDoc; i++) {
          if (reader.isDeleted(i)) {
            delCount++;
            docMap[i] = -1;
          } else
            docMap[i] = j++;
        }
      }
    }
    return docMap;
  }

  final boolean nextField() throws IOException {
    field = fields.next();
    if (field != null) {
      terms = fields.terms();
      return true;
    } else {
      return false;
    }
  }

  final boolean nextTerm() throws IOException {
    term = terms.next();
    if (term != null) {
      if (Codec.DEBUG) {
        System.out.println("  smi.next: term=" + term + " seg=" + segment);
      }
      return true;
    } else {
      if (Codec.DEBUG) {
        System.out.println("  smi.next: term=null seg=" + segment);
      }
      return false;
    }
  }
}

