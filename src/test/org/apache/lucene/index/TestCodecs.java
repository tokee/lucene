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

import org.apache.lucene.util.*;
import org.apache.lucene.index.codecs.*;
import org.apache.lucene.index.codecs.standard.*;
import org.apache.lucene.store.*;
import java.util.*;

// TODO: test multiple codecs here?

// TODO
//   - test across fields
//   - fix this test to run once for all codecs
//   - make more docs per term, to test > 1 level skipping
//   - test all combinations of payloads/not and omitTF/not
//   - test w/ different indexDivisor
//   - test field where payload length rarely changes
//   - 0-term fields
//   - seek/skip to same term/doc i'm already on
//   - mix in deleted docs
//   - seek, skip beyond end -- assert returns false
//   - seek, skip to things that don't exist -- ensure it
//     goes to 1 before next one known to exist
//   - skipTo(term)
//   - skipTo(doc)

public class TestCodecs extends LuceneTestCase {

  private Random RANDOM;
  private static String[] fieldNames = new String[] {"one", "two", "three", "four"};

  private final static int NUM_TEST_ITER = 4000;
  private final static int NUM_TEST_THREADS = 3;
  private final static int NUM_FIELDS = 4;
  private final static int NUM_TERMS_RAND = 50; // must be > 16 to test skipping
  private final static int DOC_FREQ_RAND = 500; // must be > 16 to test skipping
  private final static int TERM_DOC_FREQ_RAND = 20;

  // start is inclusive and end is exclusive
  public int nextInt(int start, int end) {
    return start + RANDOM.nextInt(end-start);
  }

  private int nextInt(int lim) {
    return RANDOM.nextInt(lim);
  }

  char[] getRandomText() {

    final int len = 1+nextInt(10);
    char[] buffer = new char[len+1];
    for(int i=0;i<len;i++) {
      buffer[i] = (char) nextInt(97, 123);
      /*
      final int t = nextInt(5);
      if (0 == t && i < len-1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) nextInt(0xd800, 0xdc00);
        // Low surrogate
        buffer[i] = (char) nextInt(0xdc00, 0xe000);
      } else if (t <= 1)
        buffer[i] = (char) nextInt(0x80);
      else if (2 == t)
        buffer[i] = (char) nextInt(0x80, 0x800);
      else if (3 == t)
        buffer[i] = (char) nextInt(0x800, 0xd800);
      else
        buffer[i] = (char) nextInt(0xe000, 0xffff);
    */
    }
    buffer[len] = 0xffff;
    return buffer;
  }

  class FieldData implements Comparable {
    final FieldInfo fieldInfo;
    final TermData[] terms;
    final boolean omitTF;
    final boolean storePayloads;

    public FieldData(String name, FieldInfos fieldInfos, TermData[] terms, boolean omitTF, boolean storePayloads) {
      this.omitTF = omitTF;
      this.storePayloads = storePayloads;
      fieldInfos.add(name, true);
      fieldInfo = fieldInfos.fieldInfo(name);
      fieldInfo.omitTermFreqAndPositions = omitTF;
      fieldInfo.storePayloads = storePayloads;
      this.terms = terms;
      for(int i=0;i<terms.length;i++)
        terms[i].field = this;
      
      Arrays.sort(terms);
    }

    public int compareTo(Object other) {
      return fieldInfo.name.compareTo(((FieldData) other).fieldInfo.name);
    }

    public void write(FieldsConsumer consumer) throws Throwable {
      if (Codec.DEBUG)
        System.out.println("WRITE field=" + fieldInfo.name);
      Arrays.sort(terms);
      final TermsConsumer termsConsumer = consumer.addField(fieldInfo);
      for(int i=0;i<terms.length;i++)
        terms[i].write(termsConsumer);
      termsConsumer.finish();
    }
  }

  class PositionData {
    int pos;
    BytesRef payload;

    PositionData(int pos, BytesRef payload) {
      this.pos = pos;
      this.payload = payload;
    }
  }

  class TermData implements Comparable {
    String text2;
    final BytesRef text;
    int[] docs;
    PositionData[][] positions;
    FieldData field;
    
    public TermData(String text, int[] docs, PositionData[][] positions) {
      this.text = new BytesRef(text);
      this.text2 = text;
      this.docs = docs;
      this.positions = positions;
    }

    public int compareTo(Object o) {
      return text2.compareTo(((TermData) o).text2);
    }    

    public void write(TermsConsumer termsConsumer) throws Throwable {
      if (Codec.DEBUG)
        System.out.println("  term=" + text2);
      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);
      for(int i=0;i<docs.length;i++) {
        final int termDocFreq;
        if (field.omitTF) {
          termDocFreq = 0;
        } else {
          termDocFreq = positions[i].length;
        }
        postingsConsumer.startDoc(docs[i], termDocFreq);
        if (!field.omitTF) {
          for(int j=0;j<positions[i].length;j++) {
            PositionData pos = positions[i][j];
            postingsConsumer.addPosition(pos.pos, pos.payload);
          }
          postingsConsumer.finishDoc();
        }
      }
      termsConsumer.finishTerm(text, docs.length);
    }
  }

  final private static String SEGMENT = "0";

  TermData[] makeRandomTerms(boolean omitTF, boolean storePayloads) {
    final int numTerms = 1+nextInt(NUM_TERMS_RAND);
    //final int numTerms = 2;
    TermData[] terms = new TermData[numTerms];

    final HashSet<String> termsSeen = new HashSet<String>();

    for(int i=0;i<numTerms;i++) {

      // Make term text
      char[] text;
      String text2;
      while(true) {
        text = getRandomText();
        text2 = new String(text, 0, text.length-1);
        if (!termsSeen.contains(text2)) {
          termsSeen.add(text2);
          break;
        }
      }
      
      final int docFreq = 1+nextInt(DOC_FREQ_RAND);
      int[] docs = new int[docFreq];
      PositionData[][] positions;

      if (!omitTF)
        positions = new PositionData[docFreq][];
      else
        positions = null;

      int docID = 0;
      for(int j=0;j<docFreq;j++) {
        docID += nextInt(1, 10);
        docs[j] = docID;

        if (!omitTF) {
          final int termFreq = 1+nextInt(TERM_DOC_FREQ_RAND);
          positions[j] = new PositionData[termFreq];
          int position = 0;
          for(int k=0;k<termFreq;k++) {
            position += nextInt(1, 10);

            final BytesRef payload;
            if (storePayloads && nextInt(4) == 0) {
              byte[] bytes = new byte[1+nextInt(5)];
              for(int l=0;l<bytes.length;l++) {
                bytes[l] = (byte) nextInt(255);
              }
              payload = new BytesRef(bytes);
            } else {
              payload = null;
            }

            positions[j][k] = new PositionData(position, payload);
          }
        }
      }

      terms[i] = new TermData(text2, docs, positions);
    }

    return terms;
  }

  public void testFixedPostings() throws Throwable {

    RANDOM = newRandom();

    final int NUM_TERMS = 100;
    TermData[] terms = new TermData[NUM_TERMS];
    for(int i=0;i<NUM_TERMS;i++) {
      int[] docs = new int[] {1};
      String text = Integer.toString(i, Character.MAX_RADIX);
      terms[i] = new TermData(text, docs, null);
    }

    final FieldInfos fieldInfos = new FieldInfos();
    
    FieldData field = new FieldData("field", fieldInfos, terms, true, false);
    FieldData[] fields = new FieldData[] {field};

    Directory dir = new MockRAMDirectory();
    write(fieldInfos, dir, fields);
    SegmentInfo si = new SegmentInfo(SEGMENT, 10000, dir, Codecs.getDefault().getWriter(null));
    si.setHasProx(false);

    FieldsProducer reader = si.getCodec().fieldsProducer(dir, fieldInfos, si, 64, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);
    
    FieldsEnum fieldsEnum = reader.iterator();
    assertNotNull(fieldsEnum.next());
    TermsEnum termsEnum = fieldsEnum.terms();
    for(int i=0;i<NUM_TERMS;i++) {
      BytesRef term = termsEnum.next();
      assertNotNull(term);
      assertEquals(terms[i].text2, term.utf8ToString());
    }
    assertNull(termsEnum.next());

    for(int i=0;i<NUM_TERMS;i++) {
      assertEquals(termsEnum.seek(new BytesRef(terms[i].text2)), TermsEnum.SeekStatus.FOUND);
    }

    assertNull(fieldsEnum.next());
  }

  public void testRandomPostings() throws Throwable {

    RANDOM = newRandom();

    final FieldInfos fieldInfos = new FieldInfos();
    
    FieldData[] fields = new FieldData[NUM_FIELDS];
    for(int i=0;i<NUM_FIELDS;i++) {
      boolean omitTF = 0==(i%3);
      boolean storePayloads = 1==(i%3);
      fields[i] = new FieldData(fieldNames[i], fieldInfos, makeRandomTerms(omitTF, storePayloads), omitTF, storePayloads);
    }

    Directory dir = new MockRAMDirectory();

    write(fieldInfos, dir, fields);
    SegmentInfo si = new SegmentInfo(SEGMENT, 10000, dir, Codecs.getDefault().getWriter(null));

    if (Codec.DEBUG) {
      System.out.println("\nTEST: now read");
    }

    FieldsProducer terms = si.getCodec().fieldsProducer(dir, fieldInfos, si, 1024, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);

    Verify[] threads = new Verify[NUM_TEST_THREADS-1];
    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i] = new Verify(fields, terms);
      threads[i].setDaemon(true);
      threads[i].start();
    }
    
    new Verify(fields, terms).run();

    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i].join();
      assert !threads[i].failed;
    }

    terms.close();
    dir.close();
  }

  private String getDesc(FieldData field, TermData term) {
    return field.fieldInfo.name + ":" + term.text2;
  }

  private String getDesc(FieldData field, TermData term, int doc) {
    return getDesc(field, term) + ":" + doc;
  }
  
  private class Verify extends Thread {
    final Fields termsDict;
    final FieldData[] fields;
    volatile boolean failed;

    Verify(FieldData[] fields, Fields termsDict) {
      this.fields = fields;
      this.termsDict = termsDict;
    }
    
    public void run() {
      try {
        _run();
      } catch (Throwable t) {
        failed = true;
        throw new RuntimeException(t);
      }
    }

    private void verifyDocs(int[] docs, PositionData[][] positions, DocsEnum docsEnum, boolean doPos) throws Throwable {
      for(int i=0;i<docs.length;i++) {
        int doc = docsEnum.nextDoc();
        assertTrue(doc != DocsEnum.NO_MORE_DOCS);
        assertEquals(docs[i], doc);
        if (doPos) {
          verifyPositions(positions[i], ((DocsAndPositionsEnum) docsEnum));
        }
      }
      assertEquals(DocsEnum.NO_MORE_DOCS, docsEnum.nextDoc());
    }

    byte[] data = new byte[10];

    private void verifyPositions(PositionData[] positions, DocsAndPositionsEnum posEnum) throws Throwable {
      for(int i=0;i<positions.length;i++) {
        int pos = posEnum.nextPosition();
        if (Codec.DEBUG) {
          System.out.println("TEST pos " + (1+i) + " of " + positions.length + " pos=" + pos);
        }
        assertEquals(positions[i].pos, pos);
        if (positions[i].payload != null) {
          assertTrue(posEnum.hasPayload());
          if (nextInt(3) < 2) {
            // Verify the payload bytes
            final BytesRef otherPayload = posEnum.getPayload();
            if (Codec.DEBUG) {
              System.out.println("TEST do check payload len=" + posEnum.getPayloadLength() + " vs " + (otherPayload == null ? "null" : otherPayload.length));
            }

            assertTrue("expected=" + positions[i].payload.toString() + " got=" + otherPayload.toString(), positions[i].payload.equals(otherPayload));
          } else {
            if (Codec.DEBUG) {
              System.out.println("TEST skip check payload len=" + posEnum.getPayloadLength());
            }
          }
        } else {
          assertFalse(posEnum.hasPayload());
        }
      }
    }

    public void _run() throws Throwable {
      
      for(int iter=0;iter<NUM_TEST_ITER;iter++) {
        final FieldData field = fields[nextInt(fields.length)];
        if (Codec.DEBUG) {
          System.out.println("verify field=" + field.fieldInfo.name);
        }

        final TermsEnum termsEnum = termsDict.terms(field.fieldInfo.name).iterator();

        // Test straight enum of the terms:
        if (Codec.DEBUG) {
          System.out.println("\nTEST: pure enum");
        }

        int upto = 0;
        while(true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (Codec.DEBUG) {
            System.out.println("check " + upto + ": " + field.terms[upto].text2);
          }
          assertTrue(new BytesRef(field.terms[upto++].text2).bytesEquals(term));
        }
        assertEquals(upto, field.terms.length);

        // Test random seek:
        if (Codec.DEBUG) {
          System.out.println("\nTEST: random seek");
        }
        TermData term = field.terms[nextInt(field.terms.length)];
        TermsEnum.SeekStatus status = termsEnum.seek(new BytesRef(term.text2));
        assertEquals(status, TermsEnum.SeekStatus.FOUND);
        assertEquals(term.docs.length, termsEnum.docFreq());
        if (field.omitTF) {
          verifyDocs(term.docs, term.positions, termsEnum.docs(null, null), false);
        } else {
          verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
        }

        // Test random seek by ord:
        int idx = nextInt(field.terms.length);
        term = field.terms[idx];
        status = termsEnum.seek(idx);
        assertEquals(status, TermsEnum.SeekStatus.FOUND);
        assertTrue(termsEnum.term().bytesEquals(new BytesRef(term.text2)));
        assertEquals(term.docs.length, termsEnum.docFreq());
        if (field.omitTF) {
          verifyDocs(term.docs, term.positions, termsEnum.docs(null, null), false);
        } else {
          verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
        }

        // Test seek to non-existent terms:
        if (Codec.DEBUG)
          System.out.println("\nTEST: seek to non-existent term");
        for(int i=0;i<100;i++) {
          char[] text = getRandomText();
          String text2 = new String(text, 0, text.length-1) + ".";
          status = termsEnum.seek(new BytesRef(text2));
          assertTrue(status == TermsEnum.SeekStatus.NOT_FOUND ||
                     status == TermsEnum.SeekStatus.END);
        }
        
        // Seek to each term, backwards:
        if (Codec.DEBUG) {
          System.out.println("\n" + Thread.currentThread().getName() + ": TEST: seek backwards through terms");
        }
        for(int i=field.terms.length-1;i>=0;i--) {
          if (Codec.DEBUG) {
            System.out.println(Thread.currentThread().getName() + ": TEST: term=" + field.terms[i].text2 + " has docFreq=" + field.terms[i].docs.length);
          }
          assertEquals(Thread.currentThread().getName() + ": field=" + field.fieldInfo.name + " term=" + field.terms[i].text2, TermsEnum.SeekStatus.FOUND, termsEnum.seek(new BytesRef(field.terms[i].text2)));
          assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
        }

        // Seek to each term by ord, backwards
        if (Codec.DEBUG) {
          System.out.println("\n" + Thread.currentThread().getName() + ": TEST: seek backwards through terms, by ord");
        }
        for(int i=field.terms.length-1;i>=0;i--) {
          if (Codec.DEBUG) {
            System.out.println(Thread.currentThread().getName() + ": TEST: term=" + field.terms[i].text2 + " has docFreq=" + field.terms[i].docs.length);
          }
          assertEquals(Thread.currentThread().getName() + ": field=" + field.fieldInfo.name + " term=" + field.terms[i].text2, TermsEnum.SeekStatus.FOUND, termsEnum.seek(i));
          assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
          assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[i].text2)));
        }

        // Seek to non-existent empty-string term
        status = termsEnum.seek(new BytesRef(""));
        assertNotNull(status);
        assertEquals(status, TermsEnum.SeekStatus.NOT_FOUND);

        // Make sure we're now pointing to first term
        assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[0].text2)));

        // Test docs enum
        if (Codec.DEBUG) {
          System.out.println("\nTEST: docs/positions");
        }
        termsEnum.seek(new BytesRef(""));
        upto = 0;
        do {
          term = field.terms[upto];
          if (nextInt(3) == 1) {
            if (Codec.DEBUG) {
              System.out.println("\nTEST [" + getDesc(field, term) + "]: iterate docs...");
            }
            DocsEnum docs = termsEnum.docs(null, null);
            DocsAndPositionsEnum postings = termsEnum.docsAndPositions(null, null);

            final DocsEnum docsEnum;
            if (postings != null) {
              docsEnum = postings;
            } else {
              docsEnum = docs;
            }
            int upto2 = -1;
            while(upto2 < term.docs.length-1) {
              // Maybe skip:
              final int left = term.docs.length-upto2;
              int doc;
              if (nextInt(3) == 1 && left >= 1) {
                int inc = 1+nextInt(left-1);
                upto2 += inc;
                if (Codec.DEBUG) {
                  System.out.println("TEST [" + getDesc(field, term) + "]: skip: " + left + " docs left; skip to doc=" + term.docs[upto2] + " [" + upto2 + " of " + term.docs.length + "]");
                }

                if (nextInt(2) == 1) {
                  doc = docsEnum.advance(term.docs[upto2]);
                  assertEquals(term.docs[upto2], doc);
                } else {
                  doc = docsEnum.advance(1+term.docs[upto2]);
                  if (doc == DocsEnum.NO_MORE_DOCS) {
                    // skipped past last doc
                    assert upto2 == term.docs.length-1;
                    break;
                  } else {
                    // skipped to next doc
                    assert upto2 < term.docs.length-1;
                    if (doc >= term.docs[1+upto2]) {
                      upto2++;
                    }
                  }
                }
              } else {
                doc = docsEnum.nextDoc();
                assertTrue(doc != -1);
                if (Codec.DEBUG) {
                  System.out.println("TEST [" + getDesc(field, term) + "]: got next doc...");
                }
                upto2++;
              }
              assertEquals(term.docs[upto2], doc);
              if (!field.omitTF) {
                assertEquals(term.positions[upto2].length, docsEnum.freq());
                if (nextInt(2) == 1) {
                  if (Codec.DEBUG) {
                    System.out.println("TEST [" + getDesc(field, term, term.docs[upto2]) + "]: check positions for doc " + term.docs[upto2] + "...");
                  }
                  verifyPositions(term.positions[upto2], postings);
                } else if (Codec.DEBUG) {
                  System.out.println("TEST: skip positions...");
                }
              } else if (Codec.DEBUG) {
                System.out.println("TEST: skip positions: omitTF=true");
              }
            }

            assertEquals(DocsEnum.NO_MORE_DOCS, docsEnum.nextDoc());

          } else if (Codec.DEBUG) {
            System.out.println("\nTEST [" + getDesc(field, term) + "]: skip docs");
          }
          upto++;

        } while (termsEnum.next() != null);

        assertEquals(upto, field.terms.length);
      }
    }
  }

  private void write(FieldInfos fieldInfos, Directory dir, FieldData[] fields) throws Throwable {

    final int termIndexInterval = nextInt(13, 27);

    SegmentWriteState state = new SegmentWriteState(null, dir, SEGMENT, fieldInfos, null, 10000, 10000, termIndexInterval,
                                                    Codecs.getDefault());

    final FieldsConsumer consumer = state.codec.fieldsConsumer(state);
    Arrays.sort(fields);
    for(int i=0;i<fields.length;i++) {
      fields[i].write(consumer);
    }
    consumer.close();
  }
}
