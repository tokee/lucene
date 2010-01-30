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

/** Consumes doc & freq, writing them using the current
 *  index file format */

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.util.BytesRef;

public final class StandardPostingsWriterImpl extends StandardPostingsWriter {
  final static String CODEC = "StandardPostingsWriterImpl";
  
  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IndexOutput freqOut;
  final IndexOutput proxOut;
  final DefaultSkipListWriter skipListWriter;
  final int skipInterval;
  final int maxSkipLevels;
  final int totalNumDocs;
  IndexOutput termsOut;

  boolean omitTermFreqAndPositions;
  boolean storePayloads;
  // Starts a new term
  long lastFreqStart;
  long freqStart;
  long lastProxStart;
  long proxStart;
  FieldInfo fieldInfo;
  int lastPayloadLength;
  int lastPosition;

  public StandardPostingsWriterImpl(SegmentWriteState state) throws IOException {
    super();
    String fileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.FREQ_EXTENSION);
    state.flushedFiles.add(fileName);
    freqOut = state.directory.createOutput(fileName);

    if (state.fieldInfos.hasProx()) {
      // At least one field does not omit TF, so create the
      // prox file
      fileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.PROX_EXTENSION);
      state.flushedFiles.add(fileName);
      proxOut = state.directory.createOutput(fileName);
    } else {
      // Every field omits TF so we will write no prox file
      proxOut = null;
    }

    totalNumDocs = state.numDocs;

    skipListWriter = new DefaultSkipListWriter(state.skipInterval,
                                               state.maxSkipLevels,
                                               state.numDocs,
                                               freqOut,
                                               proxOut);
     
    skipInterval = state.skipInterval;
    maxSkipLevels = state.maxSkipLevels;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
  }

  @Override
  public void startTerm() {
    freqStart = freqOut.getFilePointer();
    if (proxOut != null) {
      proxStart = proxOut.getFilePointer();
      // force first payload to write its length
      lastPayloadLength = -1;
    }
    skipListWriter.resetSkip();
  }

  // nocommit -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
  }

  int lastDocID;
  int df;
  
  int count;

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void addDoc(int docID, int termDocFreq) throws IOException {

    final int delta = docID - lastDocID;
    
    if (Codec.DEBUG) {
      Codec.debug("  addDoc [" + desc + "] count=" + (count++) + " docID=" + docID + " lastDocID=" + lastDocID + " delta=" + delta + " omitTF=" + omitTermFreqAndPositions + " freq=" + termDocFreq + " freq.fp=" + freqOut.getFilePointer());
    }

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
      skipListWriter.bufferSkip(df);
      if (Codec.DEBUG) {
        System.out.println("    bufferSkip lastDocID=" + lastDocID + " df=" + df + " freqFP=" + freqOut.getFilePointer() + " proxFP=" + skipListWriter.proxOutput.getFilePointer());
      }
    }

    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (omitTermFreqAndPositions) {
      freqOut.writeVInt(delta);
    } else if (1 == termDocFreq) {
      freqOut.writeVInt((delta<<1) | 1);
    } else {
      freqOut.writeVInt(delta<<1);
      freqOut.writeVInt(termDocFreq);
    }

    lastPosition = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    assert !omitTermFreqAndPositions: "omitTermFreqAndPositions is true";
    assert proxOut != null;

    if (Codec.DEBUG) {
      if (payload != null) {
        Codec.debug("    addPos [" + desc + "]: pos=" + position + " prox.fp=" + proxOut.getFilePointer() + " payload=" + payload.length + " bytes");
      } else {
        Codec.debug("    addPos [" + desc + "]: pos=" + position + " prox.fp=" + proxOut.getFilePointer());
      }
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
        proxOut.writeVInt((delta<<1)|1);
        proxOut.writeVInt(payloadLength);
      } else {
        proxOut.writeVInt(delta << 1);
      }

      if (payloadLength > 0) {
        proxOut.writeBytes(payload.bytes, payload.offset, payloadLength);
      }
    } else {
      proxOut.writeVInt(delta);
    }
  }

  @Override
  public void finishDoc() {
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {
    // nocommit -- wasteful we are counting this in two places?
    assert docCount == df;
    // mxx
    if (Codec.DEBUG) {
      Codec.debug("dw.finishTerm termsOut.fp=" + termsOut.getFilePointer() + " freqStart=" + freqStart + " df=" + df + " isIndex?=" + isIndexTerm);
    }

    if (isIndexTerm) {
      // Write absolute at seek points
      termsOut.writeVLong(freqStart);
    } else {
      // Write delta between seek points
      termsOut.writeVLong(freqStart - lastFreqStart);
    }

    lastFreqStart = freqStart;

    if (df >= skipInterval) {
      // mxx
      if (Codec.DEBUG) {
        System.out.println(Thread.currentThread().getName() + ":  writeSkip @ freqFP=" + freqOut.getFilePointer() + " freqStartFP=" + freqStart);
      }
      termsOut.writeVInt((int) (skipListWriter.writeSkip(freqOut)-freqStart));
    }
     
    if (!omitTermFreqAndPositions) {
      if (isIndexTerm) {
        // Write absolute at seek points
        termsOut.writeVLong(proxStart);
      } else {
        // Write delta between seek points
        termsOut.writeVLong(proxStart - lastProxStart);
      }
      lastProxStart = proxStart;
    }

    lastDocID = 0;
    df = 0;
    
    // nocommit
    count = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      freqOut.close();
    } finally {
      if (proxOut != null) {
        proxOut.close();
      }
    }
  }
}
