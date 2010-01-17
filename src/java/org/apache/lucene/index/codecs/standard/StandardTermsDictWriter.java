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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.DocsConsumer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.store.IndexOutput;

/**
 * Writes terms dict and interacts with docs/positions
 * consumers to write the postings files.
 *
 * The [new] terms dict format is field-centric: each field
 * has its own section in the file.  Fields are written in
 * UTF16 string comparison order.  Within each field, each
 * term's text is written in UTF16 string comparison order.
 */

public class StandardTermsDictWriter extends FieldsConsumer {

  final static String CODEC_NAME = "STANDARD_TERMS_DICT";

  // Initial format
  public static final int VERSION_START = 0;

  public static final int VERSION_CURRENT = VERSION_START;

  private final DeltaBytesWriter termWriter;

  final IndexOutput out;
  final StandardDocsConsumer consumer;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final StandardTermsIndexWriter indexWriter;
  private final List<TermsConsumer> fields = new ArrayList<TermsConsumer>();
  private final BytesRef.Comparator termComp;

  // nocommit
  private String segment;

  public StandardTermsDictWriter(StandardTermsIndexWriter indexWriter, SegmentWriteState state, StandardDocsConsumer consumer, BytesRef.Comparator termComp) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.TERMS_EXTENSION);
    this.indexWriter = indexWriter;
    this.termComp = termComp;
    out = state.directory.createOutput(termsFileName);
    indexWriter.setTermsOutput(out);
    state.flushedFiles.add(termsFileName);
    this.segment = state.segmentName;

    if (Codec.DEBUG) {
      System.out.println("stdw: write to segment=" + state.segmentName);
    }

    fieldInfos = state.fieldInfos;

    // Count indexed fields up front
    Codec.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 

    out.writeLong(0);                             // leave space for end index pointer

    termWriter = new DeltaBytesWriter(out);
    currentField = null;
    this.consumer = consumer;

    consumer.start(out);                          // have consumer write its format/header
  }

  @Override
  public TermsConsumer addField(FieldInfo field) {
    if (Codec.DEBUG) {
      System.out.println("stdw.addField: field=" + field.name);
    }
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    StandardTermsIndexWriter.FieldWriter fieldIndexWriter = indexWriter.addField(field);
    TermsConsumer terms = new TermsWriter(fieldIndexWriter, field, consumer);
    fields.add(terms);
    return terms;
  }
  
  @Override
  public void close() throws IOException {

    if (Codec.DEBUG)
      System.out.println("stdw.close seg=" + segment);

    try {
      final int fieldCount = fields.size();

      if (Codec.DEBUG)
        System.out.println("  numFields=" + fieldCount);

      final long dirStart = out.getFilePointer();

      out.writeInt(fieldCount);
      for(int i=0;i<fieldCount;i++) {
        TermsWriter field = (TermsWriter) fields.get(i);
        out.writeInt(field.fieldInfo.number);
        out.writeLong(field.numTerms);
        out.writeLong(field.termsStartPointer);
        if (Codec.DEBUG)
          System.out.println("stdw.close: field=" + field.fieldInfo.name + " numTerms=" + field.numTerms + " tis pointer=" + field.termsStartPointer);
      }
      out.seek(Codec.headerSize(CODEC_NAME));
      out.writeLong(dirStart);
    } finally {
      try {
        out.close();
      } finally {
        try {
          consumer.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  long lastIndexPointer;

  class TermsWriter extends TermsConsumer {
    final FieldInfo fieldInfo;
    final StandardDocsConsumer consumer;
    final long termsStartPointer;
    int numTerms;
    final StandardTermsIndexWriter.FieldWriter fieldIndexWriter;

    TermsWriter(StandardTermsIndexWriter.FieldWriter fieldIndexWriter, FieldInfo fieldInfo, StandardDocsConsumer consumer) {
      this.fieldInfo = fieldInfo;
      this.consumer = consumer;
      this.fieldIndexWriter = fieldIndexWriter;

      termWriter.reset();
      termsStartPointer = out.getFilePointer();
      consumer.setField(fieldInfo);
      lastIndexPointer = termsStartPointer;

      if (Codec.DEBUG) {
        System.out.println("stdw: now write field=" + fieldInfo.name);
      }
    }
    
    @Override
    public BytesRef.Comparator getComparator() {
      return termComp;
    }

    @Override
    public DocsConsumer startTerm(BytesRef text) throws IOException {
      consumer.startTerm();
      if (Codec.DEBUG) {
        consumer.desc = fieldInfo.name + ":" + text;
        System.out.println("stdw.startTerm term=" + fieldInfo.name + ":" + text + " seg=" + segment);
      }
      return consumer;
    }

    @Override
    public void finishTerm(BytesRef text, int numDocs) throws IOException {

      // mxx
      if (Codec.DEBUG) {
        // nocommit
        System.out.println(Thread.currentThread().getName() + ": stdw.finishTerm seg=" + segment + " text=" + fieldInfo.name + ":" + text + " numDocs=" + numDocs + " numTerms=" + numTerms);
      }

      if (numDocs > 0) {
        final boolean isIndexTerm = fieldIndexWriter.checkIndexTerm(text, numDocs);

        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ":  filePointer=" + out.getFilePointer() + " isIndexTerm?=" + isIndexTerm);
          System.out.println("  term bytes=" + text.toBytesString());
        }
        termWriter.write(text);
        out.writeVInt(numDocs);

        consumer.finishTerm(numDocs, isIndexTerm);
        numTerms++;
      }
    }

    // Finishes all terms in this field
    @Override
    public void finish() {
    }
  }
}