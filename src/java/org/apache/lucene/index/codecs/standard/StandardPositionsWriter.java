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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

final class StandardPositionsWriter extends StandardPositionsConsumer {
  final static String CODEC = "SingleFilePositionsPayloads";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;
  
  final StandardDocsWriter parent;
  final IndexOutput out;
  
  IndexOutput termsOut;

  boolean omitTermFreqAndPositions;
  boolean storePayloads;
  int lastPayloadLength = -1;

  // nocommit
  String desc;
  
  StandardPositionsWriter(SegmentWriteState state, StandardDocsWriter parent) throws IOException {
    this.parent = parent;
    omitTermFreqAndPositions = parent.omitTermFreqAndPositions;
    if (state.fieldInfos.hasProx()) {
      // At least one field does not omit TF, so create the
      // prox file
      final String fileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.PROX_EXTENSION);
      state.flushedFiles.add(fileName);
      out = state.directory.createOutput(fileName);
      parent.skipListWriter.setProxOutput(out);
    } else
      // Every field omits TF so we will write no prox file
      out = null;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
  }

  long proxStart;
  long lastProxStart;

  @Override
  public void startTerm() {
    proxStart = out.getFilePointer();
    lastPayloadLength = -1;
  }

  
  int lastPosition;

  /** Add a new position & payload */
  @Override
  public void add(int position, BytesRef payload) throws IOException {
    assert !omitTermFreqAndPositions: "omitTermFreqAndPositions is true";
    assert out != null;

    if (Codec.DEBUG) {
      if (payload != null)
        System.out.println("pw.addPos [" + desc + "]: pos=" + position + " fp=" + out.getFilePointer() + " payload=" + payload.length + " bytes");
      else
        System.out.println("pw.addPos [" + desc + "]: pos=" + position + " fp=" + out.getFilePointer());
    }
    
    final int delta = position - lastPosition;
    
    assert delta > 0 || position == 0 || position == -1: "position=" + position + " lastPosition=" + lastPosition;            // not quite right (if pos=0 is repeated twice we don't catch it)

    lastPosition = position;

    if (storePayloads) {
      if (Codec.DEBUG) {
        System.out.println("  store payloads");
      }
      final int payloadLength = payload == null ? 0 : payload.length;

      if (payloadLength != lastPayloadLength) {
        if (Codec.DEBUG) {
          System.out.println("  payload len change old=" + lastPayloadLength + " new=" + payloadLength);
        }

        lastPayloadLength = payloadLength;
        out.writeVInt((delta<<1)|1);
        out.writeVInt(payloadLength);
      } else {
        out.writeVInt(delta << 1);
      }

      if (payloadLength > 0) {
        out.writeBytes(payload.bytes, payload.offset, payloadLength);
      }
    } else {
      out.writeVInt(delta);
    }
  }

  void setField(FieldInfo fieldInfo) {
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    storePayloads = omitTermFreqAndPositions ? false : fieldInfo.storePayloads;
  }

  /** Called when we are done adding positions & payloads */
  @Override
  public void finishDoc() {       
    lastPosition = 0;
  }

  @Override
  public void finishTerm(boolean isIndexTerm) throws IOException {
    assert !omitTermFreqAndPositions;

    // mxx
    if (Codec.DEBUG) {
      System.out.println("poswriter finishTerm isIndex=" + isIndexTerm + " proxStart=" + proxStart + " pointer=" + termsOut.getFilePointer());
    }

    if (isIndexTerm) {
      // Write absolute at seek points
      termsOut.writeVLong(proxStart);
    } else {
      termsOut.writeVLong(proxStart-lastProxStart);
    }

    lastProxStart = proxStart;
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }
}
