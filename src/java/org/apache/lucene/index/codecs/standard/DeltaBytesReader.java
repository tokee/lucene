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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.TermRef;

import java.io.IOException;

// Handles reading incremental UTF8 encoded terms
final class DeltaBytesReader {
  final TermRef term = new TermRef();
  final IndexInput in;
  boolean started;

  DeltaBytesReader(IndexInput in) {
    this.in = in;
    term.bytes = new byte[10];
  }

  void reset(TermRef text) {
    term.copy(text);
  }

  void read() throws IOException {
    // mxx
    //System.out.println(Thread.currentThread().getName() + ":  dbr termFP=" + in.getFilePointer());
    final int start = in.readVInt();
    final int suffix = in.readVInt();
    // mxx
    //System.out.println(Thread.currentThread().getName() + ":  start=" + start + " suffix=" + suffix);
    assert start <= term.length: "start=" + start + " length=" + term.length;
    final int newLength = start+suffix;
    term.grow(newLength);
    in.readBytes(term.bytes, start, suffix);
    term.length = newLength;
  }
}
