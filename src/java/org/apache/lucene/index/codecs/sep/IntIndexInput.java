package org.apache.lucene.index.codecs.sep;

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

import java.io.IOException;

/** Defines basic API for writing ints to an IndexOutput.
 *  IntBlockCodec interacts with this API. @see
 *  IntBlockReader */
public abstract class IntIndexInput {

  public abstract Reader reader() throws IOException;

  public abstract void close() throws IOException;

  public abstract Index index() throws IOException;

  public abstract static class Index {

    // nocommit
    public String desc;

    public abstract void read(IndexInput indexIn, boolean absolute) throws IOException;

    /** Seeks primary stream to the last read offset */
    public abstract void seek(IntIndexInput.Reader stream) throws IOException;

    public abstract void set(Index other);
  }

  public static final class BulkReadResult {
    public int[] buffer;
    public int offset;
    public int len;
  };

  public abstract static class Reader {

    /** Reads next single int */
    public abstract int next() throws IOException;

    /** Reads next chunk of ints */
    public abstract BulkReadResult read(int[] buffer, int count) throws IOException;

    public abstract String descFilePointer() throws IOException;
  }
}
