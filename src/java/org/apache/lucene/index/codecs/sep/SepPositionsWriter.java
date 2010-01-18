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

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.codecs.PositionsConsumer;
import org.apache.lucene.index.codecs.Codec;

/** @lucene.experimental */
public final class SepPositionsWriter extends PositionsConsumer {

  final static String CODEC = "SepPositionsPayloads";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final SepDocsWriter parent;
  final IntIndexOutput posOut;
  final IntIndexOutput.Index posIndex;
  final IndexOutput payloadOut;

  IndexOutput termsOut;

  boolean omitTF;
  boolean storePayloads;
  int lastPayloadLength = -1;

  // nocommit
  String desc;

  public SepPositionsWriter(SegmentWriteState state, SepDocsWriter parent, IntStreamFactory factory) throws IOException {
    this.parent = parent;
    omitTF = parent.omitTF;
    if (Codec.DEBUG) {
      System.out.println("spw.create seg=" + state.segmentName + " dir=" + state.directory);
    }
    if (state.fieldInfos.hasProx()) {
      // At least one field does not omit TF, so create the

      // prox file
      final String proxFileName = IndexFileNames.segmentFileName(state.segmentName, SepCodec.POS_EXTENSION);
      posOut = factory.createOutput(state.directory, proxFileName);
      state.flushedFiles.add(proxFileName);
      posIndex = posOut.index();

      // nocommit -- only if at least one field stores
      // payloads?
      boolean success = false;
      final String payloadFileName = IndexFileNames.segmentFileName(state.segmentName, SepCodec.PAYLOAD_EXTENSION);
      try {
        payloadOut = state.directory.createOutput(payloadFileName);
        success = true;
      } finally {
        if (!success) {
          posOut.close();
        }
      }
      state.flushedFiles.add(payloadFileName);

      if (Codec.DEBUG) {
        System.out.println("  hasProx create pos=" + proxFileName + " payload=" + payloadFileName);
      }

      parent.skipListWriter.setPosOutput(posOut);
      parent.skipListWriter.setPayloadOutput(payloadOut);
    } else {
      if (Codec.DEBUG) {
        System.out.println("  no prox");
      }
      // Every field omits TF so we will write no prox file
      posIndex = null;
      posOut = null;
      payloadOut = null;
    }
  }

  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
  }

  long payloadStart;
  long lastPayloadStart;

  public void startTerm() throws IOException {
    posIndex.mark();
    payloadStart = payloadOut.getFilePointer();
    lastPayloadLength = -1;
  }

  int lastPosition;

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) throws IOException {
    assert !omitTF: "omitTF is true";
    assert posOut != null;
    if (Codec.DEBUG) {
      if (payload != null) {
        System.out.println("pw.addPos [" + desc + "]: pos=" + position + " posFP=" + posOut.descFilePointer() + " payloadFP=" + payloadOut.getFilePointer() + " payload=" + payloadLength + " bytes");
      } else {
        System.out.println("pw.addPos [" + desc + "]: pos=" + position + " posFP=" + posOut.descFilePointer() + " payloadFP=" + payloadOut.getFilePointer());
      }
    }

    final int delta = position - lastPosition;
    lastPosition = position;

    if (storePayloads) {
      if (Codec.DEBUG) {
        System.out.println("  store payload len=" + payloadLength);
      }
      if (payloadLength != lastPayloadLength) {
        if (Codec.DEBUG) {
          System.out.println("  payload len change old=" + lastPayloadLength + " new=" + payloadLength);
        }
        lastPayloadLength = payloadLength;
        // TODO: explore whether we get better compression
        // by not storing payloadLength into prox stream?
        posOut.write((delta<<1)|1);
        posOut.write(payloadLength);
      } else {
        posOut.write(delta << 1);
      }

      if (payloadLength > 0) {
        if (Codec.DEBUG) {
          System.out.println("  write @ payloadFP=" + payloadOut.getFilePointer());
        }
        payloadOut.writeBytes(payload, payloadLength);
      }
    } else {
      posOut.write(delta);
    }
  }

  void setField(FieldInfo fieldInfo) {
    omitTF = fieldInfo.omitTermFreqAndPositions;
    storePayloads = omitTF ? false : fieldInfo.storePayloads;
  }

  /** Called when we are done adding positions & payloads */
  @Override
  public void finishDoc() {       
    lastPosition = 0;
  }

  public void finishTerm(boolean isIndexTerm) throws IOException {
    assert !omitTF;

    if (Codec.DEBUG) {
      System.out.println("poswriter finishTerm isIndex=" + isIndexTerm + " pointer=" + termsOut.getFilePointer());
    }

    posIndex.write(termsOut, isIndexTerm);
    if (isIndexTerm) {
      // Write absolute at seek points
      termsOut.writeVLong(payloadStart);
    } else {
      termsOut.writeVLong(payloadStart-lastPayloadStart);
    }

    lastPayloadStart = payloadStart;
  }

  public void close() throws IOException {
    try {
      if (posOut != null) {
        posOut.close();
      }
    } finally {
      if (payloadOut != null) {
        payloadOut.close();
      }
    }
  }
}
