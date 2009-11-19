package org.apache.lucene;

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

import org.apache.lucene.util.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.codecs.*;
import org.apache.lucene.index.codecs.standard.*;
import org.apache.lucene.index.codecs.pulsing.*;
import org.apache.lucene.store.*;
import java.util.*;
import java.io.*;

/* Intentionally outside of oal.index to verify fully
   external codecs work fine */

public class TestExternalCodecs extends LuceneTestCase {

  // TODO
  //   - good improvement would be to write through to disk,
  //     and then load into ram from disk
  public static class RAMOnlyCodec extends Codec {

    // Postings state:
    static class RAMPostings extends FieldsProducer {
      final Map<String,RAMField> fieldToTerms = new TreeMap<String,RAMField>();

      public Terms terms(String field) {
        return fieldToTerms.get(field);
      }

      public FieldsEnum iterator() {
        return new RAMFieldsEnum(this);
      }

      public void close() {
      }

      public void loadTermsIndex() {
      }
    } 

    static class RAMField extends Terms {
      final String field;
      final SortedMap<String,RAMTerm> termToDocs = new TreeMap<String,RAMTerm>();
      RAMField(String field) {
        this.field = field;
      }

      public long getUniqueTermCount() {
        return termToDocs.size();
      }

      public TermsEnum iterator() {
        return new RAMTermsEnum(RAMOnlyCodec.RAMField.this);
      }
    }

    static class RAMTerm {
      final String term;
      final List<RAMDoc> docs = new ArrayList<RAMDoc>();
      public RAMTerm(String term) {
        this.term = term;
      }
    }

    static class RAMDoc {
      final int docID;
      final int[] positions;
      public RAMDoc(int docID, int freq) {
        this.docID = docID;
        positions = new int[freq];
      }
    }

    // Classes for writing to the postings state
    private static class RAMFieldsConsumer extends FieldsConsumer {

      private final RAMPostings postings;
      private final RAMTermsConsumer termsConsumer = new RAMTermsConsumer();

      public RAMFieldsConsumer(RAMPostings postings) {
        this.postings = postings;
      }

      public TermsConsumer addField(FieldInfo field) {
        RAMField ramField = new RAMField(field.name);
        postings.fieldToTerms.put(field.name, ramField);
        termsConsumer.reset(ramField);
        return termsConsumer;
      }

      public void close() {
        // TODO: finalize stuff
      }
    }

    private static class RAMTermsConsumer extends TermsConsumer {
      private RAMField field;
      private final RAMDocsConsumer docsConsumer = new RAMDocsConsumer();
      RAMTerm current;
      
      void reset(RAMField field) {
        this.field = field;
      }
        
      public DocsConsumer startTerm(TermRef text) {
        final String term = text.toString();
        current = new RAMTerm(term);
        docsConsumer.reset(current);
        return docsConsumer;
      }

      public void finishTerm(TermRef text, int numDocs) {
        // nocommit -- are we even called when numDocs == 0?
        if (numDocs > 0) {
          assert numDocs == current.docs.size();
          field.termToDocs.put(current.term, current);
        }
      }

      public void finish() {
      }
    }

    public static class RAMDocsConsumer extends DocsConsumer {
      private RAMTerm term;
      private RAMDoc current;
      private final RAMPositionsConsumer positions = new RAMPositionsConsumer();

      public void reset(RAMTerm term) {
        this.term = term;
      }
      public PositionsConsumer addDoc(int docID, int freq) {
        current = new RAMDoc(docID, freq);
        term.docs.add(current);
        positions.reset(current);
        return positions;
      }
    }

    public static class RAMPositionsConsumer extends PositionsConsumer {
      private RAMDoc current;
      int upto = 0;
      public void reset(RAMDoc doc) {
        current = doc;
        upto = 0;
      }

      public void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) {
        if (payload != null) {
          throw new UnsupportedOperationException("can't handle payloads");
        }
        current.positions[upto++] = position;
      }

      public void finishDoc() {
        assert upto == current.positions.length;
      }
    }


    // Classes for reading from the postings state
    static class RAMFieldsEnum extends FieldsEnum {
      private final RAMPostings postings;
      private final Iterator<String> it;
      private String current;

      public RAMFieldsEnum(RAMPostings postings) {
        this.postings = postings;
        this.it = postings.fieldToTerms.keySet().iterator();
      }

      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }
        return current;
      }

      public TermsEnum terms() {
        return new RAMTermsEnum(postings.fieldToTerms.get(current));
      }

      void close() {
      }
    }

    static class RAMTermsEnum extends TermsEnum {
      Iterator<String> it;
      String current;
      private final RAMField ramField;

      public RAMTermsEnum(RAMField field) {
        this.ramField = field;
      }

      public TermRef next() {
        if (it == null) {
          if (current == null) {
            it = ramField.termToDocs.keySet().iterator();
          } else {
            it = ramField.termToDocs.tailMap(current).keySet().iterator();
          }
        }
        if (it.hasNext()) {
          current = it.next();
          return new TermRef(current);
        } else {
          return null;
        }
      }

      public SeekStatus seek(TermRef term) {
        current = term.toString();
        if (ramField.termToDocs.containsKey(current)) {
          return SeekStatus.FOUND;
        } else {
          // nocommit -- right?
          if (current.compareTo(ramField.termToDocs.lastKey()) > 0) {
            return SeekStatus.END;
          } else {
            return SeekStatus.NOT_FOUND;
          }
        }
      }

      public SeekStatus seek(long ord) {
        throw new UnsupportedOperationException();
      }

      public long ord() {
        throw new UnsupportedOperationException();
      }

      public TermRef term() {
        // TODO: reuse TermRef
        return new TermRef(current);
      }

      public int docFreq() {
        return ramField.termToDocs.get(current).docs.size();
      }

      public DocsEnum docs(Bits skipDocs) {
        return new RAMDocsEnum(ramField.termToDocs.get(current), skipDocs);
      }
    }

    private static class RAMDocsEnum extends DocsEnum {
      private final RAMTerm ramTerm;
      private final Bits skipDocs;
      private final RAMPositionsEnum positions = new RAMPositionsEnum();
      private RAMDoc current;
      int upto = -1;

      public RAMDocsEnum(RAMTerm ramTerm, Bits skipDocs) {
        this.ramTerm = ramTerm;
        this.skipDocs = skipDocs;
      }

      public int advance(int targetDocID) {
        do {
          next();
        } while (upto < ramTerm.docs.size() && current.docID < targetDocID);
        return NO_MORE_DOCS;
      }

      // TODO: override bulk read, for better perf

      public int next() {
        while(true) {
          upto++;
          if (upto < ramTerm.docs.size()) {
            current = ramTerm.docs.get(upto);
            if (skipDocs == null || !skipDocs.get(current.docID)) {
              return current.docID;
            }
          } else {
            return NO_MORE_DOCS;
          }
        }
      }

      public int freq() {
        return current.positions.length;
      }

      public PositionsEnum positions() {
        positions.reset(current);
        return positions;
      }
    }

    private static final class RAMPositionsEnum extends PositionsEnum {
      private RAMDoc ramDoc;
      int upto;

      public void reset(RAMDoc ramDoc) {
        this.ramDoc = ramDoc;
        upto = 0;
      }

      public int next() {
        return ramDoc.positions[upto++];
      }

      public boolean hasPayload() {
        return false;
      }

      public int getPayloadLength() {
        return 0;
      }

      public byte[] getPayload(byte[] data, int offset) {
        return null;
      }
    }

    // Holds all indexes created
    private final Map<String,RAMPostings> state = new HashMap<String,RAMPostings>();

    public FieldsConsumer fieldsConsumer(SegmentWriteState writeState) {
      RAMPostings postings = new RAMPostings();
      RAMFieldsConsumer consumer = new RAMFieldsConsumer(postings);
      synchronized(state) {
        state.put(writeState.segmentName, postings);
      }
      return consumer;
    }

    public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos, SegmentInfo si, int readBufferSize, int indexDivisor)
      throws IOException {
      return state.get(si.name);
    }

    public void getExtensions(Collection extensions) {
    }

    public void files(Directory dir, SegmentInfo segmentInfo, Collection files) {
    }
  }

  /** Simple Codec that dispatches field-specific codecs.
   *  You must ensure every field you index has a Codec, or
   *  the defaultCodec is non null.  Also, the separate
   *  codecs cannot conflict on file names.*/
  public static class PerFieldCodecWrapper extends Codec {
    private final Map<String,Codec> fields = new HashMap<String,Codec>();
    private final Codec defaultCodec;

    public PerFieldCodecWrapper(Codec defaultCodec) {
      name = "PerField";
      this.defaultCodec = defaultCodec;
    }

    public void add(String field, Codec codec) {
      fields.put(field, codec);
    }

    Codec getCodec(String field) {
      Codec codec = fields.get(field);
      if (codec != null) {
        return codec;
      } else {
        return defaultCodec;
      }
    }
      
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      return new FieldsWriter(state);
    }

    private class FieldsWriter extends FieldsConsumer {
      private final SegmentWriteState state;
      private final Map<Codec,FieldsConsumer> codecs = new HashMap<Codec,FieldsConsumer>();
      private final Set<String> fieldsSeen = new TreeSet<String>();

      public FieldsWriter(SegmentWriteState state) {
        this.state = state;
      }

      public TermsConsumer addField(FieldInfo field) throws IOException {
        fieldsSeen.add(field.name);
        Codec codec = getCodec(field.name);

        FieldsConsumer fields = codecs.get(codec);
        if (fields == null) {
          fields = codec.fieldsConsumer(state);
          codecs.put(codec, fields);
        }
        //System.out.println("field " + field.name + " -> codec " + codec);
        return fields.addField(field);
      }

      public void close() throws IOException {
        Iterator<FieldsConsumer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().close();
        }
      }
    }

    private class FieldsReader extends FieldsProducer {

      private final Set<String> fields = new TreeSet();
      private final Map<Codec,FieldsProducer> codecs = new HashMap<Codec,FieldsProducer>();

      public FieldsReader(Directory dir, FieldInfos fieldInfos,
                          SegmentInfo si, int readBufferSize,
                          int indexDivisor) throws IOException {

        final int fieldCount = fieldInfos.size();
        for(int i=0;i<fieldCount;i++) {
          FieldInfo fi = fieldInfos.fieldInfo(i);
          if (fi.isIndexed) {
            fields.add(fi.name);
            Codec codec = getCodec(fi.name);
            if (!codecs.containsKey(codec)) {
              codecs.put(codec, codec.fieldsProducer(dir, fieldInfos, si, readBufferSize, indexDivisor));
            }
          }
        }
      }

      private final class FieldsIterator extends FieldsEnum {
        private final Iterator<String> it;
        private String current;

        public FieldsIterator() {
          it = fields.iterator();
        }

        public String next() {
          if (it.hasNext()) {
            current = it.next();
          } else {
            current = null;
          }

          return current;
        }

        public TermsEnum terms() throws IOException {
          Terms terms = codecs.get(getCodec(current)).terms(current);
          if (terms != null) {
            return terms.iterator();
          } else {
            return null;
          }
        }
      }
      
      public FieldsEnum iterator() throws IOException {
        return new FieldsIterator();
      }

      public Terms terms(String field) throws IOException {
        Codec codec = getCodec(field);

        FieldsProducer fields = codecs.get(codec);
        assert fields != null;
        return fields.terms(field);
      }

      public void close() throws IOException {
        Iterator<FieldsProducer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().close();
        }
      }

      public void loadTermsIndex() throws IOException {
        Iterator<FieldsProducer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().loadTermsIndex();
        }
      }
    }

    public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos,
                                         SegmentInfo si, int readBufferSize,
                                         int indexDivisor)
      throws IOException {
      return new FieldsReader(dir, fieldInfos, si, readBufferSize, indexDivisor);
    }

    public void files(Directory dir, SegmentInfo info, Collection files) throws IOException {
      Iterator<Codec> it = fields.values().iterator();
      while(it.hasNext()) {
        final Codec codec = it.next();
        codec.files(dir, info, files);
      }
    }

    public void getExtensions(Collection extensions) {
      Iterator<Codec> it = fields.values().iterator();
      while(it.hasNext()) {
        final Codec codec = it.next();
        codec.getExtensions(extensions);
      }
    }
  }

  public static class MyCodecs extends Codecs {
    PerFieldCodecWrapper perField;

    MyCodecs() {
      Codec ram = new RAMOnlyCodec();
      Codec pulsing = new PulsingCodec();
      perField = new PerFieldCodecWrapper(ram);
      perField.add("field2", pulsing);
      register(perField);
    }
    
    public Codec getWriter(SegmentWriteState state) {
      return perField;
    }
  }

  public void testPerFieldCodec() throws Exception {
    
    Directory dir = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, null, IndexWriter.MaxFieldLength.UNLIMITED,
                                    null, null, new MyCodecs());
    w.setMergeFactor(3);
    Document doc = new Document();
    // uses default codec:
    doc.add(new Field("field1", "this field uses the standard codec", Field.Store.NO, Field.Index.ANALYZED));
    // uses pulsing codec:
    doc.add(new Field("field2", "this field uses the pulsing codec", Field.Store.NO, Field.Index.ANALYZED));
    
    Field idField = new Field("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    for(int i=0;i<100;i++) {
      w.addDocument(doc);
      idField.setValue(""+i);
      if ((i+1)%10 == 0) {
        w.commit();
      }
    }
    w.deleteDocuments(new Term("id", "77"));

    IndexReader r = w.getReader();
    assertEquals(99, r.numDocs());
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(99, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(99, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    r.close();
    s.close();

    w.deleteDocuments(new Term("id", "44"));
    w.optimize();
    r = w.getReader();
    assertEquals(98, r.maxDoc());
    assertEquals(98, r.numDocs());
    s = new IndexSearcher(r);
    assertEquals(98, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(98, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    r.close();
    s.close();

    w.close();

    dir.close();
  }
}
