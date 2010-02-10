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
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.codecs.Codec;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg {@link DirectoryReade} or {@link
 * MultiReader}).
 *
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using {@link
 * ReaderUtil#gatherSubReaders} and then operate per-reader,
 * instead of using this class.
 *
 * @lucene.experimental
 */

public final class MultiFields extends Fields {
  private final Fields[] subs;
  private final ReaderUtil.Slice[] subSlices;
  private final Map<String,MultiTerms> terms = new HashMap<String,MultiTerms>();

  public static Fields getFields(IndexReader r) throws IOException {
    final IndexReader[] subs = r.getSequentialSubReaders();
    if (subs == null) {
      // already an atomic reader
      return r.fields();
    } else if (subs.length == 1) {
      return getFields(subs[0]);
    } else {

      Fields currentFields = r.retrieveFields();
      if (currentFields == null) {
      
        final List<Fields> fields = new ArrayList<Fields>();
        final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();
        ReaderUtil.gatherSubFields(null, fields, slices, r, 0);

        if (fields.size() == 0) {
          return null;
        } else if (fields.size() == 1) {
          currentFields = fields.get(0);
        } else {
          currentFields = new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                         slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
        }
        r.storeFields(currentFields);
      }
      return currentFields;
    }
  }

  public static Terms getTerms(IndexReader r, String field) throws IOException {
    final Fields fields = getFields(r);
    if (fields != null) {
      return fields.terms(field);
    } else {
      return null;
    }
  }

  /** Returns {@link DocsEnum} for the specified field &
   *  term.  This may return null, for example if either the
   *  field or term does not exist. */
  public static DocsEnum getTermDocsEnum(IndexReader r, Bits skipDocs, String field, BytesRef term) throws IOException {

    assert field != null;
    assert term != null;
    final Fields fields = getFields(r);
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        if (Codec.DEBUG) {
          System.out.println("mf.termDocsEnum field=" + field + " term=" + term + " terms=" + terms);
        }
        final DocsEnum docs = terms.docs(skipDocs, term, null);
        if (Codec.DEBUG) {
          System.out.println("mf.termDocsEnum field=" + field + " docs=" +docs);
        }
        return docs;
      }
    }

    return null;
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  field & term.  This may return null, for example if
   *  either the field or term does not exist. */
  public static DocsAndPositionsEnum getTermPositionsEnum(IndexReader r, Bits skipDocs, String field, BytesRef term) throws IOException {
    assert field != null;
    assert term != null;

    final Fields fields = getFields(r);
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        if (Codec.DEBUG) {
          System.out.println("mf.termPositionsEnum field=" + field + " term=" + term + " terms=" + terms);
        }
        final DocsAndPositionsEnum postings = terms.docsAndPositions(skipDocs, term, null);
        if (Codec.DEBUG) {
          System.out.println("mf.termPositionsEnum field=" + field + " postings=" +postings);
        }
        return postings;
      }
    }

    return null;
  }


  public MultiFields(Fields[] subs, ReaderUtil.Slice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
  }

  @Override
  public FieldsEnum iterator() throws IOException {

    final List<FieldsEnum> fieldsEnums = new ArrayList<FieldsEnum>();
    final List<ReaderUtil.Slice> fieldsSlices = new ArrayList<ReaderUtil.Slice>();
    for(int i=0;i<subs.length;i++) {
      final FieldsEnum subFieldsEnum = subs[i].iterator();
      if (subFieldsEnum != null) {
        fieldsEnums.add(subFieldsEnum);
        fieldsSlices.add(subSlices[i]);
      }
    }
    if (fieldsEnums.size() == 0) {
      return null;
    } else {
      return new MultiFieldsEnum(fieldsEnums.toArray(FieldsEnum.EMPTY_ARRAY),
                                 fieldsSlices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
    }
  }

  @Override
  public Terms terms(String field) throws IOException {

    final MultiTerms result;

    if (!terms.containsKey(field)) {

      // Lazy init: first time this field is requested, we
      // create & add to terms:
      final List<Terms> subs2 = new ArrayList<Terms>();
      final List<ReaderUtil.Slice> slices2 = new ArrayList<ReaderUtil.Slice>();

      // Gather all sub-readers that share this field
      for(int i=0;i<subs.length;i++) {
        Terms subTerms = subs[i].terms(field);
        if (subTerms != null) {
          subs2.add(subTerms);
          slices2.add(subSlices[i]);
        }
      }
      if (subs2.size() == 0) {
        result = null;
        terms.put(field, null);
      } else {
        result = new MultiTerms(subs2.toArray(Terms.EMPTY_ARRAY),
                                slices2.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
      }
      terms.put(field, result);
    } else {
      result = terms.get(field);
    }

    return result;
  }
}

