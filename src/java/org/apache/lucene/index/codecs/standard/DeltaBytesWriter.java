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

import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

final class DeltaBytesWriter {

  private final UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result(); //nocommit: not read

  private byte[] lastBytes = new byte[10];
  private int lastLength;
  final IndexOutput out;

  DeltaBytesWriter(IndexOutput out) {
    this.out = out;
  }

  void reset() {
    lastLength = 0;
  }

  void write(byte[] bytes, int length) throws IOException {
    int start = 0;
    final int limit = length < lastLength ? length : lastLength;
    while(start < limit) {
      if (bytes[start] != lastBytes[start])
        break;
      start++;
    }

    final int suffix = length - start;
    // mxx
    //System.out.println(Thread.currentThread().getName() + ":  dbw start=" + start + " suffix=" + suffix + " outFP=" + out.getFilePointer());

    out.writeVInt(start);                       // prefix
    out.writeVInt(suffix);                      // suffix
    out.writeBytes(bytes, start, suffix);
    if (lastBytes.length < bytes.length) {
      lastBytes = ArrayUtil.grow(lastBytes, bytes.length);
    }
    System.arraycopy(bytes, start, lastBytes, start, suffix);
    lastLength = length;
  }
}
