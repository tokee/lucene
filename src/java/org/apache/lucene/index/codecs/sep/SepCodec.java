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
import java.util.Collection;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DocsConsumer;
import org.apache.lucene.index.codecs.DocsProducer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardTermsDictReader;
import org.apache.lucene.index.codecs.standard.StandardTermsDictWriter;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexWriter;
import org.apache.lucene.store.Directory;

public class SepCodec extends Codec {

  public SepCodec() {
    name = "Sep";
  }

  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {

    DocsConsumer docsWriter = new SepDocsWriter(state, new SingleIntFactory());

    boolean success = false;
    StandardTermsIndexWriter indexWriter;
    try {
      indexWriter = new SimpleStandardTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        docsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new StandardTermsDictWriter(indexWriter, state, docsWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          docsWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  final static String DOC_EXTENSION = "doc";
  final static String SKIP_EXTENSION = "skp";
  final static String FREQ_EXTENSION = "frq";
  final static String POS_EXTENSION = "pos";
  final static String PAYLOAD_EXTENSION = "pyl";

  public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos, SegmentInfo si, int readBufferSize, int indexDivisor) throws IOException {

    DocsProducer docsReader = new SepDocsReader(dir, si, readBufferSize, new SingleIntFactory());

    StandardTermsIndexReader indexReader;
    boolean success = false;
    try {
      indexReader = new SimpleStandardTermsIndexReader(dir,
                                                       fieldInfos,
                                                       si.name,
                                                       indexDivisor);
      success = true;
    } finally {
      if (!success) {
        docsReader.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new StandardTermsDictReader(indexReader,
                                                       dir, fieldInfos, si.name,
                                                       docsReader,
                                                       readBufferSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          docsReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  public void files(Directory dir, SegmentInfo segmentInfo, Collection files) {
    SepDocsReader.files(segmentInfo, files);
    StandardTermsDictReader.files(segmentInfo, files);
    SimpleStandardTermsIndexReader.files(segmentInfo, files);
  }

  public void getExtensions(Collection extensions) {
    getSepExtensions(extensions);
  }

  public static void getSepExtensions(Collection extensions) {
    extensions.add(DOC_EXTENSION);
    extensions.add(FREQ_EXTENSION);
    extensions.add(SKIP_EXTENSION);
    extensions.add(POS_EXTENSION);
    extensions.add(PAYLOAD_EXTENSION);
    StandardTermsDictReader.getExtensions(extensions);
    SimpleStandardTermsIndexReader.getIndexExtensions(extensions);
  }
}