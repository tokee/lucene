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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.codecs.Codec;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/** @lucene.experimental */
public class SimpleStandardTermsIndexWriter extends StandardTermsIndexWriter {
  final private IndexOutput out;

  final static String CODEC_NAME = "SIMPLE_STANDARD_TERMS_INDEX";
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final private int termIndexInterval;

  private final List<SimpleFieldWriter> fields = new ArrayList<SimpleFieldWriter>();
  private final FieldInfos fieldInfos; // unread
  private IndexOutput termsOut;

  final private String segment;

  public SimpleStandardTermsIndexWriter(SegmentWriteState state) throws IOException {
    final String indexFileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.TERMS_INDEX_EXTENSION);
    state.flushedFiles.add(indexFileName);
    this.segment = state.segmentName;
    termIndexInterval = state.termIndexInterval;
    out = state.directory.createOutput(indexFileName);
    Codec.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    fieldInfos = state.fieldInfos;

    // Placeholder for dir offset
    out.writeLong(0);
    out.writeInt(termIndexInterval);
    termWriter = new DeltaBytesWriter(out);
  }

  @Override
  public void setTermsOutput(IndexOutput termsOut) {
    this.termsOut = termsOut;
  }
  
  final private DeltaBytesWriter termWriter;

  @Override
  public FieldWriter addField(FieldInfo field) {
    SimpleFieldWriter writer = new SimpleFieldWriter(field);
    fields.add(writer);
    return writer;
  }

  private class SimpleFieldWriter extends FieldWriter {
    final FieldInfo fieldInfo;
    int numIndexTerms;
    private long lastTermsPointer;
    final long indexStart;
    private int numTerms;

    SimpleFieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      indexStart = out.getFilePointer();
      termWriter.reset();
    }

    @Override
    public boolean checkIndexTerm(BytesRef text, int docFreq) throws IOException {
      // First term is first indexed term:
      if (0 == (numTerms++ % termIndexInterval)) {
        final long termsPointer = termsOut.getFilePointer();
        if (Codec.DEBUG) {
          Codec.debug("sstiw.checkIndexTerm write index field=" + fieldInfo.name + " term=" + text + " termsFP=" + termsPointer + " numIndexTerms=" + numIndexTerms + " outFP=" + out.getFilePointer());
        }
        termWriter.write(text);
        out.writeVLong(termsPointer - lastTermsPointer);
        lastTermsPointer = termsPointer;
        numIndexTerms++;
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public void close() throws IOException {
    final long dirStart = out.getFilePointer();
    if (Codec.DEBUG) {
      System.out.println("sstiw.close seg=" + segment + " dirStart=" + dirStart);
    }
    final int fieldCount = fields.size();

    out.writeInt(fieldCount);
    for(int i=0;i<fieldCount;i++) {
      SimpleFieldWriter field = fields.get(i);
      if (Codec.DEBUG) {
        System.out.println("sstiw.close save field=" + field.fieldInfo.name + " numIndexTerms=" + field.numIndexTerms);
      }
      out.writeInt(field.fieldInfo.number);
      out.writeInt(field.numIndexTerms);
      out.writeLong(field.indexStart);
    }
    out.seek(Codec.headerSize(CODEC_NAME));
    out.writeLong(dirStart);
    if (Codec.DEBUG) {
      System.out.println(" writeDirStart " + dirStart + " @ " + Codec.headerSize(CODEC_NAME));
    }
    out.close();
  }
}