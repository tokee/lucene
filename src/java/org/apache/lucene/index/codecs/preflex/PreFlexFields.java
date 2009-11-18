package org.apache.lucene.index.codecs.preflex;

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
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PositionsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

public class PreFlexFields extends FieldsProducer {

  // nocommit -- needed public by SegmentReader
  public final TermInfosReader tis;

  // nocomit -- needed public by SR
  public final IndexInput freqStream;
  // nocomit -- needed public by SR
  public final IndexInput proxStream;
  final private FieldInfos fieldInfos;
  final TreeMap<String,FieldInfo> fields = new TreeMap<String,FieldInfo>();

  PreFlexFields(Directory dir, FieldInfos fieldInfos, SegmentInfo info, int readBufferSize, int indexDivisor)
    throws IOException {
    tis = new TermInfosReader(dir, info.name, fieldInfos, readBufferSize, indexDivisor);    
    this.fieldInfos = fieldInfos;

    // make sure that all index files have been read or are kept open
    // so that if an index update removes them we'll still have them
    freqStream = dir.openInput(info.name + ".frq", readBufferSize);
    boolean anyProx = false;
    final int numFields = fieldInfos.size();
    for(int i=0;i<numFields;i++) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
      if (fieldInfo.isIndexed) {
        fields.put(fieldInfo.name, fieldInfo);
        if (!fieldInfo.omitTermFreqAndPositions) {
          anyProx = true;
        }
      }
    }

    if (anyProx) {
      proxStream = dir.openInput(info.name + ".prx", readBufferSize);
    } else {
      proxStream = null;
    }
  }

  static void files(Directory dir, SegmentInfo info, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.TERMS_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.TERMS_INDEX_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.FREQ_EXTENSION));
    //System.out.println("seg=" + info.name + " hasProx?=" + info.getHasProx());
    if (info.getHasProx()) {
      // LUCENE-1739: for certain versions of 2.9-dev,
      // hasProx would be incorrectly computed during
      // indexing as true, and then stored into the segments
      // file, when it should have been false.  So we do the
      // extra check, here:
      final String prx = IndexFileNames.segmentFileName(info.name, PreFlexCodec.PROX_EXTENSION);
      if (dir.fileExists(prx)) {
        files.add(prx);
      }
    }
  }

  @Override
  public FieldsEnum iterator() {
    return new Fields();
  }

  @Override
  public Terms terms(String field) {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi != null) {
      return new PreTerms(fi);
    } else {
      return null;
    }
  }

  @Override
  public void loadTermsIndex() throws IOException {
    // nocommit -- todo
  }

  @Override
  public void close() throws IOException {
    tis.close();
  }

  private class Fields extends FieldsEnum {
    Iterator<FieldInfo> it;
    FieldInfo current;
    private PreTermsEnum lastTermsEnum;

    public Fields() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        current = it.next();
        return current.name;
      } else {
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      final PreTermsEnum terms;
      if (lastTermsEnum != null) {
        // Carry over SegmentTermsEnum
        terms = new PreTermsEnum(current, lastTermsEnum.terms);
      } else {
        terms = new PreTermsEnum(current);
      }
      lastTermsEnum = terms;
      return terms;
    }
  }
  
  private class PreTerms extends Terms {
    final FieldInfo fieldInfo;
    PreTerms(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }
    @Override
    public TermsEnum iterator() {
      //System.out.println("pff.init create no context");
      return new PreTermsEnum(fieldInfo);
    }
  }

  private class PreTermsEnum extends TermsEnum {
    private SegmentTermEnum terms;
    private final FieldInfo fieldInfo;
    private PreDocsEnum docsEnum;
    private boolean skipNext;
    private TermRef current;

    PreTermsEnum(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      terms = tis.terms();
    }

    PreTermsEnum(FieldInfo fieldInfo, SegmentTermEnum terms) {
      this.fieldInfo = fieldInfo;
      this.terms = terms;
      skipNext = true;
      if (Codec.DEBUG) {
        System.out.println("pff.terms.init field=" + fieldInfo.name);
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
    public SeekStatus seek(TermRef term) throws IOException {
      if (Codec.DEBUG) {
        System.out.println("pff.seek term=" + term);
      }
      terms = tis.terms(new Term(fieldInfo.name, term.toString()));
      final Term t = terms.term();
      //System.out.println("  got to term=" + t  + " field eq?=" + (t.field() == fieldInfo.name) + " term eq?=" +
      //term.equals(new TermRef(t.text())));

      // nocommit -- reuse TermRef instance
      final TermRef tr;
      if (t != null) {
        tr = new TermRef(t.text());
      } else {
        tr = null;
      }

      if (t != null && t.field() == fieldInfo.name && term.termEquals(tr)) {
        current = tr;
        return SeekStatus.FOUND;
      } else if (t == null || t.field() != fieldInfo.name) {
        current = null;
        return SeekStatus.END;
      } else {
        current = tr;
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public TermRef next() throws IOException {
      if (skipNext) {
        // nocommit -- is there a cleaner way?
        skipNext = false;
        // nocommit -- reuse TermRef
        current = new TermRef(terms.term().text());
        return current;
      }
      if (terms.next()) {
        final Term t = terms.term();
        if (Codec.DEBUG) {
          System.out.println("pff.next term=" + t);
        }
        if (t.field() == fieldInfo.name) {
          // nocommit -- reuse TermRef instance
          if (Codec.DEBUG) {
            System.out.println("  ok");
          }
          current = new TermRef(t.text());
          return current;
        } else {
          // Crossed into new field
          if (Codec.DEBUG) {
            System.out.println("  stop (new field " + t.field());
          }
          current = null;
          return null;
        }
      } else {
        current = null;
        return null;
      }
    }

    @Override
    public TermRef term() {
      return current;
    }

    @Override
    public int docFreq() {
      return terms.docFreq();
    }

    @Override
    public DocsEnum docs(Bits skipDocs) throws IOException {
      return new PreDocsEnum(skipDocs, terms);
    }
  }

  private final class PreDocsEnum extends DocsEnum {
    final private SegmentTermDocs docs;
    final private SegmentTermPositions pos;
    private SegmentTermDocs current;
    final private PrePositionsEnum prePos;

    PreDocsEnum(Bits skipDocs, Term t) throws IOException {
      current = docs = new SegmentTermDocs(freqStream, skipDocs, tis, fieldInfos);
      pos = new SegmentTermPositions(freqStream, proxStream, skipDocs, tis, fieldInfos);
      prePos = new PrePositionsEnum(pos);
      docs.seek(t);
      pos.seek(t);
    }

    PreDocsEnum(Bits skipDocs, SegmentTermEnum te) throws IOException {
      current = docs = new SegmentTermDocs(freqStream, skipDocs, tis, fieldInfos);
      pos = new SegmentTermPositions(freqStream, proxStream, skipDocs, tis, fieldInfos);
      prePos = new PrePositionsEnum(pos);
      docs.seek(te);
      pos.seek(te);
    }

    @Override
    public int next() throws IOException {
      if (Codec.DEBUG) {
        System.out.println("pff.docs.next");
      }
      if (current.next()) {
        return current.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (current.skipTo(target)) {
        return current.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return current.freq();
    }

    @Override
    public int read(int[] docIDs, int[] freqs) throws IOException {
      if (current != docs) {
        docs.skipTo(current.doc());
        current = docs;
      }
      return current.read(docIDs, freqs);
    }

    @Override
    public PositionsEnum positions() throws IOException {
      if (current != pos) {
        pos.skipTo(docs.doc());
        current = pos;
      }
      return prePos;
    }
  }

  private final class PrePositionsEnum extends PositionsEnum {
    final private SegmentTermPositions pos;
    PrePositionsEnum(SegmentTermPositions pos) {
      this.pos = pos;
    }

    @Override
    public int next() throws IOException {
      return pos.nextPosition();
    }

    @Override
    public int getPayloadLength() {
      return pos.getPayloadLength();
    }

    @Override
    public boolean hasPayload() {
      return pos.isPayloadAvailable();
    }

    @Override
    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return pos.getPayload(data, offset);
    }
  }
}
