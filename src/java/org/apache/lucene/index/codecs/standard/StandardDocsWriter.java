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
import org.apache.lucene.index.codecs.DocsConsumer;
import org.apache.lucene.index.codecs.PositionsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.codecs.Codec;

public final class StandardDocsWriter extends DocsConsumer {
  final static String CODEC = "SingleFileDocFreqSkip";
  
  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IndexOutput out;
  final StandardPositionsWriter posWriter;
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
  FieldInfo fieldInfo;

  public StandardDocsWriter(SegmentWriteState state) throws IOException {
    super();
    final String fileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.FREQ_EXTENSION);
    state.flushedFiles.add(fileName);
    out = state.directory.createOutput(fileName);
    totalNumDocs = state.numDocs;

    // nocommit -- abstraction violation
    skipListWriter = new DefaultSkipListWriter(state.skipInterval,
                                               state.maxSkipLevels,
                                               state.numDocs,
                                               out,
                                               null);
     
    skipInterval = state.skipInterval;
    maxSkipLevels = state.maxSkipLevels;

    posWriter = new StandardPositionsWriter(state, this);
  }

  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    posWriter.start(termsOut);
  }

  public void startTerm() {
    freqStart = out.getFilePointer();
    if (!omitTermFreqAndPositions)
      posWriter.startTerm();
    skipListWriter.resetSkip();
  }

  // nocommit -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
    posWriter.setField(fieldInfo);
  }

  int lastDocID;
  int df;
  
  int count;

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  public PositionsConsumer addDoc(int docID, int termDocFreq) throws IOException {

    final int delta = docID - lastDocID;
    
    if (Codec.DEBUG) {
      System.out.println("  dw.addDoc [" + desc + "] count=" + (count++) + " docID=" + docID + " lastDocID=" + lastDocID + " delta=" + delta + " omitTF=" + omitTermFreqAndPositions + " freq=" + termDocFreq + " freqPointer=" + out.getFilePointer());
    }

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      // TODO: abstraction violation
      skipListWriter.setSkipData(lastDocID, storePayloads, posWriter.lastPayloadLength);
      skipListWriter.bufferSkip(df);
      if (Codec.DEBUG) {
        System.out.println("    bufferSkip lastDocID=" + lastDocID + " df=" + df + " freqFP=" + out.getFilePointer() + " proxFP=" + skipListWriter.proxOutput.getFilePointer());
      }
    }

    // nocommit -- move this assert up above; every consumer
    // shouldn't have to check for this bug:
    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (omitTermFreqAndPositions) {
      out.writeVInt(delta);
    } else if (1 == termDocFreq) {
      out.writeVInt((delta<<1) | 1);
    } else {
      out.writeVInt(delta<<1);
      out.writeVInt(termDocFreq);
    }

    // nocommit
    if (Codec.DEBUG) {
      ((StandardPositionsWriter) posWriter).desc = desc + ":" + docID;
    }

    if (omitTermFreqAndPositions) {
      return null;
    } else {
      return posWriter;
    }
  }

  /** Called when we are done adding docs to this term */
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {
    // nocommit -- wasteful we are counting this in two places?
    assert docCount == df;
    // mxx
    if (Codec.DEBUG) {
      System.out.println(Thread.currentThread().getName() + ": dw.finishTerm termsOut pointer=" + termsOut.getFilePointer() + " freqStart=" + freqStart + " df=" + df + " isIndex?=" + isIndexTerm);
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
        System.out.println(Thread.currentThread().getName() + ":  writeSkip @ freqFP=" + out.getFilePointer() + " freqStartFP=" + freqStart);
      }
      termsOut.writeVLong(skipListWriter.writeSkip(out)-freqStart);
    }
     
    if (!omitTermFreqAndPositions) {
      posWriter.finishTerm(isIndexTerm);
    }


    lastDocID = 0;
    df = 0;
    
    // nocommit
    count = 0;
  }

  public void close() throws IOException {
    if (Codec.DEBUG)
      System.out.println("docs writer close pointer=" + out.getFilePointer());
    try {
      out.close();
    } finally {
      posWriter.close();
    }
  }
}
