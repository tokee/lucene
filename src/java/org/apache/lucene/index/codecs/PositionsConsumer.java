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

// nocommit -- split into generic vs standardtermsdict
public abstract class PositionsConsumer {

  public abstract void start(IndexOutput termsOut) throws IOException;

  public abstract void startTerm() throws IOException;

  /** Add a new position & payload.  If payloadLength > 0
   *  you must read those bytes from the IndexInput.  NOTE:
   *  you must fully consume the byte[] payload, since
   *  caller is free to reuse it on subsequent calls. */
  public abstract void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) throws IOException;

  /** Called when we are done adding positions & payloads
   * for each doc */
  public abstract void finishDoc() throws IOException;

  public abstract void finishTerm(boolean isIndexTerm) throws IOException;
  
  public abstract void close() throws IOException;
}
