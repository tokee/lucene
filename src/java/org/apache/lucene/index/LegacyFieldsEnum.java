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

import java.io.IOException;
import org.apache.lucene.util.Bits;

/** Implements flex API (FieldsEnum/TermsEnum) on top of
 *  pre-flex API.  Used only for IndexReader impls outside
 *  Lucene's core. */
class LegacyFieldsEnum extends FieldsEnum {
  private final IndexReader r;
  private TermEnum terms;
  private String field;

  public LegacyFieldsEnum(IndexReader r) throws IOException {
    this.r = r;
    terms = r.terms();
  }

  private void doSeek(Term t) throws IOException {
    terms.close();
    terms = r.terms(t);
  }

  @Override
  public String next() throws IOException {

    if (field != null) {
      final Term seekTo = new Term(field, "\uFFFF");
      doSeek(seekTo);
    }
    if (terms.term() != null) {
      String newField = terms.term().field;
      assert !newField.equals(field);
      field = newField;
      return field;
    } else {
      return null;
    }
  }

  @Override
  public TermsEnum terms() throws IOException {
    return new LegacyTermsEnum(r, field);
  }

  static class LegacyTermsEnum extends TermsEnum {
    private final IndexReader r;
    private final String field;
    private TermEnum terms;
    private TermRef current;
    private final TermRef tr = new TermRef();

    LegacyTermsEnum(IndexReader r, String field) throws IOException {
      this.r = r;
      this.field = field;
      this.terms = r.terms(new Term(field, ""));
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return TermRef.getUTF8SortedAsUTF16Comparator();
    }

    @Override
    public SeekStatus seek(TermRef text) throws IOException {
      
      // nocommit -- should we optimize for "silly seek"
      // cases, here?  ie seek to term you're already on, to
      // very next term , etc.
      terms.close();
      terms = r.terms(new Term(field, text.toString()));

      final Term t = terms.term();
      if (t == null) {
        current = null;
        return SeekStatus.END;
      } else if (t.field() == field) {
        tr.copy(t.text());
        current = tr;
        if (text.termEquals(tr)) {
          return SeekStatus.FOUND;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      } else {
        return SeekStatus.END;
      }
    }

    @Override
    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TermRef next() throws IOException {
      if (terms.next()) {
        if (terms.term().field == field) {
          tr.copy(terms.term().text());
          current = tr;
        } else {
          current = null;
        }
        return current;
      } else {
        current = null;
        return null;
      }
    }

    @Override
    public TermRef term() {
      return current;
    }

    /*
    public String text() {
      return terms.term().text;
    }
    */

    @Override
    public int docFreq() {
      return terms.docFreq();
    }

    @Override
    public DocsEnum docs(Bits skipDocs) throws IOException {
      return new LegacyDocsEnum(r, field, terms.term(), skipDocs);
    }

    public void close() throws IOException {
      terms.close();
    }
  }

  // Emulates flex on top of legacy API
  private static class LegacyDocsEnum extends DocsEnum {
    final TermDocs td;
    final Term term;
    final IndexReader r;
    final String field;
    final Bits skipDocs;

    TermPositions tp;
    int doc = -1;

    LegacyDocsEnum(IndexReader r, String field, Term term, Bits skipDocs) throws IOException {
      this.r = r;
      this.field = field;
      this.term = term;
      td = r.termDocs(term);
      this.skipDocs = skipDocs;
    }

    // nocommit -- must enforce skipDocs... but old API will
    // always secretly skip deleted docs, and we can't work
    // around that for external readers?
    @Override
    public int nextDoc() throws IOException {
      if (td.next()) {
        return doc = td.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (td.skipTo(target)) {
        return doc = td.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return td.freq();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      return td.read(docs, freqs);
    }

    public void close() throws IOException {
      td.close();
    }

    LegacyPositionsEnum lpe;

    @Override
    public PositionsEnum positions() throws IOException {
      if (tp == null) {
        tp = r.termPositions(term);
        lpe = new LegacyPositionsEnum(tp);
      } else {
        tp.seek(term);
      }
      return lpe;
    }
  }

  // Emulates flex on top of legacy API
  private static class LegacyPositionsEnum extends PositionsEnum {

    final TermPositions tp;

    LegacyPositionsEnum(TermPositions tp) {
      this.tp = tp;
    }

    @Override
    public int next() throws IOException {
      return tp.nextPosition();
    }

    @Override
    public int getPayloadLength() {
      return tp.getPayloadLength();
    }

    @Override
    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return tp.getPayload(data, offset);
    }

    @Override
    public boolean hasPayload() {
      return tp.isPayloadAvailable();
    }
  }
}