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

/** Implements new API (FieldsEnum/TermsEnum) on top of old
 *  API.  Used only for IndexReader impls outside Lucene's
 *  core. */
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

  /*
  public boolean seek(String field) throws IOException {
    this.field = field;
    doSeek(new Term(field, ""));
    return terms.term() != null && terms.term().field.equals(field);
  }
  */

  public String next() throws IOException {

    final Term seekTo = new Term(field, "\uFFFF");

    doSeek(seekTo);
    if (terms.term() != null) {
      String newField = terms.term().field;
      assert !newField.equals(field);
      field = newField;
      return field;
    } else {
      return null;
    }
  }

  public TermsEnum terms() throws IOException {
    return new LegacyTermsEnum(r, field);
  }

  public void close() throws IOException {
    terms.close();
  }

  // Emulates flex on top of legacy API
  static class LegacyTermsEnum extends TermsEnum {
    private final IndexReader r;
    private final String field;
    private TermEnum terms;
    private TermRef current;

    LegacyTermsEnum(IndexReader r, String field) throws IOException {
      this.r = r;
      this.field = field;
      this.terms = r.terms(new Term(field, ""));
    }

    public SeekStatus seek(TermRef text) throws IOException {

      // nocommit: too slow?
      terms.close();
      terms = r.terms(new Term(field, text.toString()));
      final Term t = terms.term();
      if (t == null) {
        current = null;
        return SeekStatus.END;
      } else {
        final TermRef tr = new TermRef(t.text());
        if (text.termEquals(tr)) {
          current = tr;
          return SeekStatus.FOUND;
        } else {
          // nocommit reuse TermRef instance
          current = tr;
          return SeekStatus.NOT_FOUND;
        }
      }
    }

    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    public TermRef next() throws IOException {
      if (terms.next()) {
        // nocommit -- reuse TermRef instance
        current = new TermRef(terms.term().text());
        return current;
      } else {
        current = null;
        return null;
      }
    }

    public TermRef term() {
      return current;
    }

    /*
    public String text() {
      return terms.term().text;
    }
    */

    public int docFreq() {
      return terms.docFreq();
    }

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
    public int next() throws IOException {
      if (td.next()) {
        return td.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    public int advance(int target) throws IOException {
      if (td.skipTo(target)) {
        return td.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    public int freq() {
      return td.freq();
    }

    public int read(int[] docs, int[] freqs) throws IOException {
      return td.read(docs, freqs);
    }

    public void close() throws IOException {
      td.close();
    }

    LegacyPositionsEnum lpe;

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

    public int next() throws IOException {
      return tp.nextPosition();
    }

    public int getPayloadLength() {
      return tp.getPayloadLength();
    }

    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return tp.getPayload(data, offset);
    }

    public boolean hasPayload() {
      return tp.isPayloadAvailable();
    }
  }
}