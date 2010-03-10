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
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** @lucene.experimental */
public abstract class Codec {

  public static boolean DEBUG = false;

  private static final int CODEC_HEADER = 0x1af65;

  /** Unique name that's used to retrieve this codec when
   *  reading the index */
  public String name;

  /** Writes a new segment */
  public abstract FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

  public static void debug(String s, String desc) {
    if (desc != null) {
      System.out.println(Thread.currentThread().getName()+ " [" + desc + "]:" + s);
    } else {
      System.out.println(Thread.currentThread().getName() + ": " + s);
    }
  }
  public static void debug(String s) {
    debug(s, null);
  }

  /** Reads a segment.  NOTE: by the time this call
   *  returns, it must hold open any files it will need to
   *  use; else, those files may be deleted. */
  public abstract FieldsProducer fieldsProducer(SegmentReadState state) throws IOException;

  /** Gathers files associated with this segment */
  public abstract void files(Directory dir, SegmentInfo segmentInfo, Set<String> files) throws IOException;

  /** Records all file extensions this codec uses */
  public abstract void getExtensions(Set<String> extensions);

  /** @return Actual version of the file */
  public static int checkHeader(IndexInput in, String codec, int version) throws IOException {

    // Safety to guard against reading a bogus string:
    int header = in.readInt();
    if (header != CODEC_HEADER) {
      throw new CorruptIndexException("codec header mismatch: " + header + " vs " + CODEC_HEADER);
    }

    final String actualCodec = in.readString();
    if (!codec.equals(actualCodec)) {
      throw new CorruptIndexException("codec mismatch: expected '" + codec + "' but got '" + actualCodec + "'");
    }

    int actualVersion = in.readInt();
    if (actualVersion > version) {
      throw new CorruptIndexException("version '" + actualVersion + "' is too new (expected <= '" + version + "'");
    }

    return actualVersion;
  }

  public static void writeHeader(IndexOutput out, String codec, int version) throws IOException {
    final long start = out.getFilePointer();
    out.writeInt(CODEC_HEADER);
    out.writeString(codec);
    out.writeInt(version);

    // So we can easily compute headerSize (below)
    if (out.getFilePointer()-start != codec.length() + 9) {
      throw new IllegalArgumentException("codec must be simple ASCII, less than 128 characters in length [got " + codec + "]");
    }
  }

  public static int headerSize(String codec) {
    return 9 + codec.length();
  }
}
