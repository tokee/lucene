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
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.DocsConsumer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.UnicodeUtil;

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

  // nocommit
  private String segment;

  public StandardTermsDictWriter(StandardTermsIndexWriter indexWriter, SegmentWriteState state, StandardDocsConsumer consumer) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentName, StandardCodec.TERMS_EXTENSION);
    this.indexWriter = indexWriter;
    out = state.directory.createOutput(termsFileName);
    indexWriter.setTermsOutput(out);
    state.flushedFiles.add(termsFileName);
    this.segment = state.segmentName;

    if (Codec.DEBUG) {
      System.out.println("stdw: write to segment=" + state.segmentName);
    }

    fieldInfos = state.fieldInfos;

    // Count indexed fields up front
    final int numFields = fieldInfos.size();
    Codec.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 

    out.writeLong(0);                             // leave space for end index pointer

    termWriter = new DeltaBytesWriter(out);
    currentField = null;
    this.consumer = consumer;

    consumer.start(out);                          // have consumer write its format/header
  }

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

  private final UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();

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
    
    public DocsConsumer startTerm(char[] text, int start) throws IOException {
      consumer.startTerm();
      if (Codec.DEBUG) {
        // nocommit
        int len = 0;
        while(text[start+len] != 0xffff) {
          len++;
        }
        consumer.desc = fieldInfo.name + ":" + new String(text, start, len);
        System.out.println("stdw.startTerm term=" + fieldInfo.name + ":" + new String(text, start, len) + " seg=" + segment);
      }
      return consumer;
    }

    public void finishTerm(char[] text, int start, int numDocs) throws IOException {

      // mxx
      if (Codec.DEBUG) {
        // nocommit
        int len = 0;
        while(text[start+len] != 0xffff) {
          len++;
        }
        System.out.println(Thread.currentThread().getName() + ": stdw.finishTerm seg=" + segment + " text=" + fieldInfo.name + ":" + new String(text, start, len) + " numDocs=" + numDocs + " numTerms=" + numTerms);
      }

      if (numDocs > 0) {
        // TODO: we could do this incrementally
        UnicodeUtil.UTF16toUTF8(text, start, utf8);

        final boolean isIndexTerm = fieldIndexWriter.checkIndexTerm(utf8.result, utf8.length, numDocs);

        // mxx
        if (Codec.DEBUG) {
          System.out.println(Thread.currentThread().getName() + ":  filePointer=" + out.getFilePointer() + " isIndexTerm?=" + isIndexTerm);
          TermRef tr = new TermRef();
          tr.bytes = utf8.result;
          tr.length = utf8.length;
          System.out.println("  term bytes=" + tr.toBytesString());
        }
        termWriter.write(utf8.result, utf8.length);
        out.writeVInt(numDocs);

        consumer.finishTerm(numDocs, isIndexTerm);
        numTerms++;
      }
    }

    // Finishes all terms in this field
    public void finish() {
    }
  }
}