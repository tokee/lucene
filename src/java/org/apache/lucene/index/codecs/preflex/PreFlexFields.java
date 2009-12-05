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
import org.apache.lucene.index.CompoundFileReader;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

/** Exposes flex API on a pre-flex index, as a codec. */
public class PreFlexFields extends FieldsProducer {

  // nocommit -- needed public by SegmentReader
  public TermInfosReader tis;
  public final TermInfosReader tisNoIndex;

  // nocomit -- needed public by SR
  public final IndexInput freqStream;
  // nocomit -- needed public by SR
  public final IndexInput proxStream;
  final private FieldInfos fieldInfos;
  private final SegmentInfo si;
  final TreeMap<String,FieldInfo> fields = new TreeMap<String,FieldInfo>();
  private final Directory dir;
  private final int readBufferSize;
  private Directory cfsReader;

  PreFlexFields(Directory dir, FieldInfos fieldInfos, SegmentInfo info, int readBufferSize, int indexDivisor)
    throws IOException {

    si = info;
    TermInfosReader r = new TermInfosReader(dir, info.name, fieldInfos, readBufferSize, indexDivisor);    
    if (indexDivisor == -1) {
      tisNoIndex = r;
    } else {
      tisNoIndex = null;
      tis = r;
    }
    this.readBufferSize = readBufferSize;
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

    this.dir = dir;
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

  synchronized private TermInfosReader getTermsDict() {
    if (tis != null) {
      return tis;
    } else {
      return tisNoIndex;
    }
  }

  @Override
  synchronized public void loadTermsIndex(int indexDivisor) throws IOException {
    if (tis == null) {
      Directory dir0;
      if (si.getUseCompoundFile()) {
        // In some cases, we were originally opened when CFS
        // was not used, but then we are asked to open the
        // terms reader with index, the segment has switched
        // to CFS

        // nocommit -- not clean that I open my own CFS
        // reader here; caller should pass it in?
        if (!(dir instanceof CompoundFileReader)) {
          dir0 = cfsReader = new CompoundFileReader(dir, si.name + "." + IndexFileNames.COMPOUND_FILE_EXTENSION, readBufferSize);
        } else {
          dir0 = dir;
        }
        dir0 = cfsReader;
      } else {
        dir0 = dir;
      }

      tis = new TermInfosReader(dir0, si.name, fieldInfos, readBufferSize, indexDivisor);
    }
  }

  @Override
  public void close() throws IOException {
    if (tis != null) {
      tis.close();
    }
    if (tisNoIndex != null) {
      tisNoIndex.close();
    }
    if (cfsReader != null) {
      cfsReader.close();
    }
  }

  private class Fields extends FieldsEnum {
    Iterator<FieldInfo> it;
    FieldInfo current;
    private SegmentTermEnum lastTermEnum;
    private int fieldCount;

    public Fields() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        fieldCount++;
        current = it.next();
        return current.name;
      } else {
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      final PreTermsEnum terms;
      if (lastTermEnum != null) {
        // Re-use SegmentTermEnum to avoid seeking for
        // linear scan (done by merging)
        terms = new PreTermsEnum(current, lastTermEnum);
      } else {
        // If fieldCount is 1 then the terms enum can simply
        // start at the start of the index (need not seek to
        // the current field):
        terms = new PreTermsEnum(current, fieldCount != 1);
        lastTermEnum = terms.terms;
      }
      return terms;
    }
  }
  
  private class PreTerms extends Terms {
    final FieldInfo fieldInfo;
    PreTerms(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      //System.out.println("pff.init create no context");
      return new PreTermsEnum(fieldInfo, true);
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return TermRef.getUTF8SortedAsUTF16Comparator();
    }
  }

  private class PreTermsEnum extends TermsEnum {
    private SegmentTermEnum terms;
    private final FieldInfo fieldInfo;
    private PreDocsEnum docsEnum; // nocommit -- unused
    private boolean skipNext;
    private TermRef current;
    private final TermRef scratchTermRef = new TermRef();

    // Pass needsSeek=false if the field is the very first
    // field in the index -- this is used for linear scan of
    // the index, eg when merging segments:
    PreTermsEnum(FieldInfo fieldInfo, boolean needsSeek) throws IOException {
      this.fieldInfo = fieldInfo;
      if (!needsSeek) {
        terms = getTermsDict().terms();
      } else {
        terms = getTermsDict().terms(new Term(fieldInfo.name, ""));
        skipNext = true;
      }
    }

    PreTermsEnum(FieldInfo fieldInfo, SegmentTermEnum terms) throws IOException {
      this.fieldInfo = fieldInfo;
      if (terms.term() == null || terms.term().field() != fieldInfo.name) {
        terms = getTermsDict().terms(new Term(fieldInfo.name, ""));
      } else {
        // Carefully avoid seeking in the linear-scan case,
        // because segment doesn't load/need the terms dict
        // index during merging.  If the terms is already on
        // our field, it must be because it had seeked to
        // exhaustion on the last field
        this.terms = terms;
      }
      skipNext = true;
      if (Codec.DEBUG) {
        System.out.println("pff.terms.init field=" + fieldInfo.name);
      }
    }

    @Override
    public TermRef.Comparator getTermComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return TermRef.getUTF8SortedAsUTF16Comparator();
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
      terms = getTermsDict().terms(new Term(fieldInfo.name, term.toString()));
      final Term t = terms.term();

      final TermRef tr;
      if (t != null) {
        tr = scratchTermRef;
        scratchTermRef.copy(t.text());
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
        scratchTermRef.copy(terms.term().text());
        current = scratchTermRef;
        return current;
      }
      if (terms.next()) {
        final Term t = terms.term();
        if (Codec.DEBUG) {
          System.out.println("pff.next term=" + t);
        }
        if (t.field() == fieldInfo.name) {
          if (Codec.DEBUG) {
            System.out.println("  ok");
          }
          scratchTermRef.copy(t.text());
          current = scratchTermRef;
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
      current = docs = new SegmentTermDocs(freqStream, skipDocs, getTermsDict(), fieldInfos);
      pos = new SegmentTermPositions(freqStream, proxStream, skipDocs, getTermsDict(), fieldInfos);
      prePos = new PrePositionsEnum(pos);
      docs.seek(t);
      pos.seek(t);
    }

    PreDocsEnum(Bits skipDocs, SegmentTermEnum te) throws IOException {
      current = docs = new SegmentTermDocs(freqStream, skipDocs, getTermsDict(), fieldInfos);
      pos = new SegmentTermPositions(freqStream, proxStream, skipDocs, getTermsDict(), fieldInfos);
      prePos = new PrePositionsEnum(pos);
      docs.seek(te);
      pos.seek(te);
    }

    @Override
    public int nextDoc() throws IOException {
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
    public int docID() {
      return current.doc();
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
