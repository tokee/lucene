package org.apache.lucene.index.codecs.sep;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
u * contributor license agreements.  See the NOTICE file distributed with
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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PositionsConsumer;
import org.apache.lucene.index.codecs.standard.StandardDocsConsumer;
import org.apache.lucene.store.IndexOutput;

/** Writes frq to .frq, docs to .doc, pos to .pos, payloads
 *  to .pyl, skip data to .skp
 *
 * @lucene.experimental */
public final class SepDocsWriter extends StandardDocsConsumer {
  final static String CODEC = "SepDocFreqSkip";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IntIndexOutput freqOut;
  final IntIndexOutput.Index freqIndex;

  final IntIndexOutput docOut;
  final IntIndexOutput.Index docIndex;

  final IndexOutput skipOut;
  IndexOutput termsOut;

  final SepPositionsWriter posWriter;
  final SepSkipListWriter skipListWriter;
  final int skipInterval;
  final int maxSkipLevels;
  final int totalNumDocs;

  boolean storePayloads;
  boolean omitTF;

  // Starts a new term
  long lastSkipStart;

  FieldInfo fieldInfo;

  public SepDocsWriter(SegmentWriteState state, IntStreamFactory factory) throws IOException {
    super();

    final String frqFileName = IndexFileNames.segmentFileName(state.segmentName, SepCodec.FREQ_EXTENSION);
    state.flushedFiles.add(frqFileName);
    freqOut = factory.createOutput(state.directory, frqFileName);
    freqIndex = freqOut.index();

    final String docFileName = IndexFileNames.segmentFileName(state.segmentName, SepCodec.DOC_EXTENSION);
    state.flushedFiles.add(docFileName);
    docOut = factory.createOutput(state.directory, docFileName);
    docIndex = docOut.index();

    final String skipFileName = IndexFileNames.segmentFileName(state.segmentName, SepCodec.SKIP_EXTENSION);
    state.flushedFiles.add(skipFileName);
    skipOut = state.directory.createOutput(skipFileName);

    if (Codec.DEBUG) {
      System.out.println("dw.init: create frq=" + frqFileName + " doc=" + docFileName + " skip=" + skipFileName);
    }

    totalNumDocs = state.numDocs;

    // nocommit -- abstraction violation
    skipListWriter = new SepSkipListWriter(state.skipInterval,
                                           state.maxSkipLevels,
                                           state.numDocs,
                                           freqOut, docOut,
                                           null, null);

    skipInterval = state.skipInterval;
    maxSkipLevels = state.maxSkipLevels;

    posWriter = new SepPositionsWriter(state, this, factory);
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    // nocommit -- just ask skipper to "start" here
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    posWriter.start(termsOut);
  }

  @Override
  public void startTerm() throws IOException {
    docIndex.mark();
    if (!omitTF) {
      freqIndex.mark();
      posWriter.startTerm();
    }
    skipListWriter.resetSkip(docIndex, freqIndex, posWriter.posIndex);
  }

  // nocommit -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTF = fieldInfo.omitTermFreqAndPositions;
    skipListWriter.setOmitTF(omitTF);
    storePayloads = fieldInfo.storePayloads;
    posWriter.setField(fieldInfo);
  }

  int lastDocID;
  int df;

  int count;

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public PositionsConsumer addDoc(int docID, int termDocFreq) throws IOException {

    final int delta = docID - lastDocID;

    if (Codec.DEBUG) {
      System.out.println("  dw.addDoc [" + desc + "] count=" + (count++) + " docID=" + docID + " lastDocID=" + lastDocID + " delta=" + delta + " omitTF=" + omitTF + " freq=" + termDocFreq);
    }

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      // TODO: abstraction violation
      // nocommit -- awkward we have to make these two
      // separate calls to skipper
      skipListWriter.setSkipData(lastDocID, storePayloads, posWriter.lastPayloadLength);
      skipListWriter.bufferSkip(df);

      if (Codec.DEBUG) {
        System.out.println("    bufferSkip lastDocID=" + lastDocID +
                           " df=" + df +
                           " docFP=" + docOut.descFilePointer() + 
                           " freqFP=" + freqOut.descFilePointer() + 
                           " posFP=" + posWriter.posOut.descFilePointer() + 
                           " payloadFP=" + skipListWriter.payloadOutput.getFilePointer() + 
                           " payloadLen=" + posWriter.lastPayloadLength);
      }
    }

    lastDocID = docID;
    docOut.write(delta);
    if (!omitTF) {
      freqOut.write(termDocFreq);
    }

    // nocommit
    if (Codec.DEBUG) {
      ((SepPositionsWriter) posWriter).desc = desc + ":" + docID;
    }

    if (omitTF) {
      return null;
    } else {
      return posWriter;
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {

    long skipPos = skipOut.getFilePointer();

    // nocommit -- wasteful we are counting this in two places?
    assert docCount == df;
    if (Codec.DEBUG) {
      System.out.println("dw.finishTerm termsFP=" + termsOut.getFilePointer() + " df=" + df + " skipPos=" + skipPos);
    }

    if (!omitTF) {
      freqIndex.write(termsOut, isIndexTerm);
    }
    docIndex.write(termsOut, isIndexTerm);

    if (df >= skipInterval) {
      if (Codec.DEBUG) {
        System.out.println("  writeSkip skipPos=" + skipPos + " lastSkipPos=" + lastSkipStart);
      }
      
      skipListWriter.writeSkip(skipOut);
    }

    if (isIndexTerm) {
      termsOut.writeVLong(skipPos);
      lastSkipStart = skipPos;
    } else if (df >= skipInterval) {
      termsOut.writeVLong(skipPos-lastSkipStart);
      lastSkipStart = skipPos;
    }

    if (!omitTF) {
      posWriter.finishTerm(isIndexTerm);
    }

    lastDocID = 0;
    df = 0;

    // nocommit
    count = 0;
  }

  @Override
  public void close() throws IOException {
    if (Codec.DEBUG)
      System.out.println("dw.close skipFP=" + skipOut.getFilePointer());
    try {
      freqOut.close();
    } finally {
      try {
        docOut.close();
      } finally {
        try {
          skipOut.close();
        } finally {
          posWriter.close();
        }
      }
    }
  }
}
