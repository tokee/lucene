package org.apache.lucene.index;

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

import java.io.*;
import java.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;

public class TestFlexExternalReader extends LuceneTestCase {

  // Delegates to a "normal" IndexReader, making it look
  // "external", to force testing of the "flex API on
  // external reader" layer
  private final static class ExternalReader extends IndexReader {
    private final IndexReader r;
    public ExternalReader(IndexReader r) {
      this.r = r;
    }

    public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
      return r.getTermFreqVectors(docNumber);
    }

    public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
      return r.getTermFreqVector(docNumber, field);
    }

    public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
      r.getTermFreqVector(docNumber, field, mapper);
    }

    public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
      r.getTermFreqVector(docNumber, mapper);
    }

    public int numDocs() {
      return r.numDocs();
    }

    public int maxDoc() {
      return r.maxDoc();
    }

    public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
      return r.document(n, fieldSelector);
    }

    public boolean isDeleted(int n) {
      return r.isDeleted(n);
    }

    public boolean hasDeletions() {
      return r.hasDeletions();
    }

    public byte[] norms(String field) throws IOException {
      return r.norms(field);
    }

    public void norms(String field, byte[] bytes, int offset) 
      throws IOException {
      r.norms(field, bytes, offset);
    }
    
    protected  void doSetNorm(int doc, String field, byte value)
      throws CorruptIndexException, IOException {
      r.doSetNorm(doc, field, value);
    }

    public TermEnum terms() throws IOException {
      return r.terms();
    }

    public TermEnum terms(Term t) throws IOException {
      return r.terms(t);
    }

    public int docFreq(Term t) throws IOException {
      return r.docFreq(t);
    }

    public TermDocs termDocs() throws IOException {
      return r.termDocs();
    }

    public TermPositions termPositions() throws IOException {
      return r.termPositions();
    }

    public void doDelete(int docID) throws IOException {
      r.doDelete(docID);
    }

    public void doUndeleteAll() throws IOException {
      r.doUndeleteAll();
    }

    protected void doCommit(Map<String, String> commitUserData) throws IOException {
      r.doCommit(commitUserData);
    }

    protected void doClose() throws IOException {
      r.doClose();
    }

    public Collection<String> getFieldNames(FieldOption fldOption) {
      return r.getFieldNames(fldOption);
    }
  }

  public void testExternalReader() throws Exception {
    Directory d = new MockRAMDirectory();

    final int DOC_COUNT = 177;

    IndexWriter w = new IndexWriter(d, new WhitespaceAnalyzer(),
                                    IndexWriter.MaxFieldLength.UNLIMITED);
    w.setMaxBufferedDocs(7);
    Document doc = new Document();
    doc.add(new Field("field1", "this is field1", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field2", "this is field2", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field3", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field4", "bbb", Field.Store.NO, Field.Index.ANALYZED));
    for(int i=0;i<DOC_COUNT;i++) {
      w.addDocument(doc);
    }

    IndexReader r = new ExternalReader(w.getReader());

    TermRef field1Term = new TermRef("field1");
    TermRef field2Term = new TermRef("field2");

    assertEquals(DOC_COUNT, r.maxDoc());
    assertEquals(DOC_COUNT, r.numDocs());
    assertEquals(DOC_COUNT, r.docFreq(new Term("field1", "field1")));
    assertEquals(DOC_COUNT, r.docFreq("field1", field1Term));

    Fields fields = r.fields();
    Terms terms = fields.terms("field1");
    TermsEnum termsEnum = terms.iterator();
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(field1Term));

    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seek(field2Term));
    assertTrue(new TermRef("is").termEquals(termsEnum.term()));

    terms = fields.terms("field2");
    termsEnum = terms.iterator();
    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seek(field1Term));
    assertTrue(termsEnum.term().termEquals(field2Term));

    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(field2Term));

    termsEnum = fields.terms("field3").iterator();
    assertEquals(TermsEnum.SeekStatus.END, termsEnum.seek(new TermRef("bbb")));

    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(new TermRef("aaa")));
    assertNull(termsEnum.next());

    r.close();
    w.close();
    d.close();
  }
}
