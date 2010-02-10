package org.apache.lucene.util;

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

/**
 * Concatenates multiple Bits together, on every lookup.
 *
 * <p><b>NOTE</b>: This is very costly, as every lookup must
 * do a binary search to locate the right sub-reader.
 */
public final class MultiBits implements Bits {
  private final Bits[] subs;

  // length is 1+subs.length (the last entry has the maxDoc):
  private final int[] starts;

  public MultiBits(Bits[] subs, int[] starts) {
    this.subs = subs;
    this.starts = starts;
  }

  private boolean checkLength(int reader, int doc) {
    final int length = starts[1+reader]-starts[reader];
    assert doc - starts[reader] < length: "doc=" + doc + " reader=" + reader + " starts[reader]=" + starts[reader] + " length=" + length;
    return true;
  }

  public boolean get(int doc) {
    final int reader = ReaderUtil.subIndex(doc, starts);
    final Bits bits = subs[reader];
    if (bits == null) {
      return false;
    } else {
      assert checkLength(reader, doc);
      return bits.get(doc-starts[reader]);
    }
  }

  public Bits getMatchingSub(ReaderUtil.Slice slice) {
    int reader = ReaderUtil.subIndex(slice.start, starts);
    if (starts[reader] == slice.start && starts[1+reader] == slice.start+slice.length) {
      return subs[reader];
    } else {
      return null;
    }
  }

  public int length() {
    return starts[starts.length-1];
  }
}
