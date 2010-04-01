package org.apache.lucene.index;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.*;

public class TestExposed extends LuceneTestCase {

  static final char[] CHARS = // Used for random content
          ("abcdefghijklmnopqrstuvwxyzæøåéèëöíêô" +
              "ABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅÉÈËÊÔÓ" +
                  "1234567890     ").toCharArray();
  static final File INDEX_LOCATION =
          new File(System.getProperty("java.io.tmpdir"), "exposed_index");
  //        new File("/home/te/projects/lucene/exposed_index");
//          new File("/mnt/bulk/exposed_index");
  public static final int DOCCOUNT = 30000;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
//    deleteIndex();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
//    deleteIndex();
  }
  private void deleteIndex() {
    if (INDEX_LOCATION.exists()) {
      for (File file: INDEX_LOCATION.listFiles()) {
        file.delete();
      }
      INDEX_LOCATION.delete();
    }
  }
                                     
  private ExposedReader createExposedSegment() throws IOException {
    createIndex(INDEX_LOCATION, DOCCOUNT, Arrays.asList("a", "b"), 40, 1);
    IndexReader reader = IndexReader.open(
            FSDirectory.open(INDEX_LOCATION), true);
    return SegmentReader.getOnlySegmentReader(reader);
  }
  private ExposedReader createExposedMultiSegment() throws IOException {
    createIndex(INDEX_LOCATION, DOCCOUNT, Arrays.asList("a", "b"), 40, 10);
    IndexReader reader = IndexReader.open(
            FSDirectory.open(INDEX_LOCATION), true);
    return (DirectoryReader)reader; // Directory iff multiple segments
  }

  public void testSingleSegmentPlainIteratorRequest() throws Exception {
    Collator comparator = Collator.getInstance(new Locale("da"));

/*  Comparator<Object> comparator = new Comparator<Object>() {
    public int compare(Object o1, Object o2) {
      return ((String)o1).compareTo((String)o2);
    }
  }; */

  ExposedReader reader = createExposedSegment();

 //   ExposedReader reader = SegmentReader.getOnlySegmentReader(IndexReader.open(
 //           FSDirectory.open(INDEX_LOCATION), true));
    testPlainIteratorRequest(reader, comparator);
  }

  public void testMultipleSegmentsPlainIteratorRequest() throws Exception {
    Collator comparator = Collator.getInstance(new Locale("da"));
    ExposedReader reader = createExposedMultiSegment();
    testPlainIteratorRequest(reader, comparator);
  }

  private void testPlainIteratorRequest(
      ExposedReader reader, Comparator<Object> comparator) throws Exception {
    long startTime = System.currentTimeMillis();
    Iterator<ExposedReader.ExposedTuple> iterator = reader.getExposedTuples(
        "foo", comparator, "b", false);
    System.out.println(
        "Got iterator in " + (System.currentTimeMillis() - startTime) + "ms");

    startTime = System.currentTimeMillis();
    long count = 0;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    System.out.println("Iterated " + count + " elements in "
        + (System.currentTimeMillis() - startTime) + "ms");
    assertTrue("There should be some elements", count > 0);

    startTime = System.currentTimeMillis();
    iterator = reader.getExposedTuples(
        "foo", Collator.getInstance(new Locale("da")), "b", false);
    System.out.println("Re-requested iterator in "
        + (System.currentTimeMillis() - startTime) + "ms");

    String last = null;
    while (iterator.hasNext()) {
      ExposedReader.ExposedTuple ot = iterator.next();
//      System.out.println(ot.ordinal);
      if (last == null) {
        last = ot.term.text;
      }
      assertTrue("Terms should be in order: " + last + " <= " + ot.term.text,
          comparator.compare(last, ot.term.text) <= 0);
      last = ot.term.text;
    }

    startTime = System.currentTimeMillis();
    reader.getExposedTuples("bar", comparator, "b", false);
    System.out.println("Got new iterator in "
        + (System.currentTimeMillis() - startTime) + "ms");
  }

  public void testDocIDIteratorRequest() throws Exception {
    Collator collator = Collator.getInstance(new Locale("da"));
    ExposedReader reader = createExposedSegment();

    long startTime = System.currentTimeMillis();
    ExposedReader.ExposedIterator iterator = reader.getExposedTuples(
        "bar2", collator, "b", true);
    System.out.println("Got new docID iterator in "
        + (System.currentTimeMillis() - startTime) + "ms");

    startTime = System.currentTimeMillis();
    ExposedReader.ExposedTuple last = null;
    long count = 0;
    while (iterator.hasNext()) {
      ExposedReader.ExposedTuple current = iterator.next();
      if (last == null) {
        last = current;
      }
      count++;
      assertTrue("The docID should not be equal to -1", current.docID != -1);
      assertTrue(String.format("" +
          "Previous term was %s, current is %s. Previous term should come " +
          "before current in comparator order.",
          last.term.text, current.term.text),
          collator.compare(last.term.text, current.term.text) <= 0);
    }
    System.out.println("Iterated " + count + " docIDs in "
        + (System.currentTimeMillis() - startTime) + "ms");
    assertTrue("There should be some docIDs", count > 0);
  }

  public void testUtilGetSortArrays() throws Exception {
    final String SORT_FIELD = "b";
    Collator collator = Collator.getInstance(new Locale("da"));
    ExposedReader reader = createExposedSegment();
    System.out.println("Requesting sortArrays");
    final ExposedUtil.SortArrays sortArrays =
        ExposedUtil.getSortArrays(reader, "foo", SORT_FIELD, collator);
    System.out.println(String.format(
        "Got sortArrays with %d docOrder (%d unique) and %d termOrder (%d " +
            "unique)",
        sortArrays.docOrder.size(), ExposedUtil.countUnique(sortArrays.docOrder),
        sortArrays.termOrder.size(), ExposedUtil.countUnique(sortArrays.termOrder)));

    System.out.println("Testing termOrder");
    String last = null;
    long unique = 0;
    for (int sortedTermIndex = 0 ;
         sortedTermIndex < sortArrays.termOrder.size() ;
         sortedTermIndex++) {
      String current =
          reader.getTermText((int)sortArrays.termOrder.get(sortedTermIndex));
      if (last == null) {
        last = current;
        unique++;
      }
      assertTrue(String.format(
          "Previous term String was '%s', current is '%s'. Previous term should " +
          "come before current in comparator order.", last, current),
          collator.compare(last, current) <= 0);
      unique += current == null || last.equals(current) ? 0 : 1;
      last = current;
    }
    System.out.println("Checked " + unique + " unique terms");

    System.out.println("Testing docOrder");
    for (int docID = 0 ; docID < sortArrays.docOrder.size() ; docID++) {
      long currentDocOrder = sortArrays.docOrder.get(docID);
      assertTrue(String.format("" +
          "The termOrder index for docID %d i docOrder should be < total " +
          "number of unique terms for the field %s (%d), but was %d", 
          docID, SORT_FIELD, DOCCOUNT, sortArrays.docOrder.get(docID)),
          currentDocOrder < DOCCOUNT);
    }

    System.out.println("Testing docOrder and termOrder");
    // Start by sorting docIDs according to their order
    final List<Integer> docIDs =
        new ArrayList<Integer>(((IndexReader)reader).maxDoc());
    for (int i = 0 ; i < docIDs.size() ; i++) {
      docIDs.set(i, i);
    }
    Collections.sort(docIDs, new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
        return (int)(sortArrays.docOrder.get(o1) - sortArrays.docOrder.get(o2));
      }
    });

    last = null;
    int lastOrdinal = -1;
    int lastSortIndex = -1;
    for (int docID: docIDs) {
      int currentSortIndex = (int)sortArrays.docOrder.get(docID);
      int currentOrdinal = (int)sortArrays.termOrder.get(currentSortIndex);
      String current = reader.getTermText(currentOrdinal);
      if (last == null) {
        last = current;
        lastOrdinal = currentOrdinal;
        lastSortIndex = currentSortIndex;
      }
      assertTrue(String.format(
          "Previous term String was '%s' (sortIndex %d, ordinal %d), " +
              "current is '%s' (sortIndex %d, ordinal %d) at docID %d. "
              + "Previous term should come before current in comparator order.",
          last, lastSortIndex, lastOrdinal, current, currentSortIndex,
          currentOrdinal, docID), 
          collator.compare(last, current) <= 0);
      last = current;
      lastOrdinal = currentOrdinal;
      lastSortIndex = currentSortIndex;
    }
  }

  public void testExposedSearch() throws Exception {
    final int HITS = 50;
    final String SORT_FIELD = "b";

    createIndex(
        INDEX_LOCATION, DOCCOUNT, Arrays.asList("a", SORT_FIELD), 40, 1);
    IndexReader reader = IndexReader.open(
        FSDirectory.open(INDEX_LOCATION), true);
    IndexSearcher searcher = new IndexSearcher(reader);
    @SuppressWarnings({"deprecation"})
    QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "all",
        new StandardAnalyzer(Version.LUCENE_CURRENT));

    ExposedFieldComparatorSource exposedFCS =
        new ExposedFieldComparatorSource(reader, new Locale("da"));
    Sort mySort = new Sort(new SortField(SORT_FIELD, exposedFCS));

    TopFieldDocs topDocs = searcher.search(
        qp.parse("all").weight(searcher), null, HITS, mySort, true);
    ExposedUtil.SortArrays sortArrays = ExposedUtil.getSortArrays(
        (ExposedReader)reader, "foo",
        SORT_FIELD, Collator.getInstance(new Locale("da")));
    List<Integer> topX = getTopX((ExposedReader)reader, sortArrays, HITS);

/*    for (int i = 0 ; i < sortArrays.docOrder.size() & i < 31 ; i++) {
      System.out.println(i + ":" + sortArrays.docOrder.get(i));
    }
  */

    for (int i = 0 ; i < Math.min(HITS, topDocs.scoreDocs.length) ; i++) {
      System.out.println(String.format(
          "Hit #%2d: docID=%5d, field %s='%s'. " +
              "Direct docID=%d, termOrderIndex=%d, term ordinal=%d, " +
              "term '%s'",
          i+1, topDocs.scoreDocs[i].doc, SORT_FIELD,
          ((FieldDoc)topDocs.scoreDocs[i]).fields[0],
          topX.get(i),
          sortArrays.docOrder.get(topX.get(i)),
          sortArrays.termOrder.get((int)sortArrays.docOrder.get(topX.get(i))),
          ((ExposedReader) reader).getTermText(
              (int)sortArrays.termOrder.get(
                  (int)sortArrays.docOrder.get(topX.get(i))))));
    }

    reader.close();
  }

  private List<Integer> getTopX(
      ExposedReader reader, final ExposedUtil.SortArrays sortArrays, int topX) {
    System.out.println("Testing docOrder and termOrder");
    // Start by sorting docIDs according to their order
    int docs = ((IndexReader)reader).maxDoc();
    final List<Integer> docIDs = new ArrayList<Integer>(docs);
    for (int i = 0 ; i < docs ; i++) {
      docIDs.add(i, i);
    }
    Collections.sort(docIDs, new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
        return (int)(sortArrays.docOrder.get(o1) - sortArrays.docOrder.get(o2));
      }
    });
    return docIDs.subList(0, topX);
  }

  private void createIndex(
      File location, int docCount, List<String> fields, int fieldContentLength,
      int maxSegments) throws IOException {
    long startTime = System.nanoTime();
    Random random = new Random(87);
    IndexWriter writer = new IndexWriter(FSDirectory.open(
            location), new SimpleAnalyzer(),
            true, IndexWriter.MaxFieldLength.UNLIMITED);
    writer.setRAMBufferSizeMB(16);

    for (int docID = 0 ; docID < docCount ; docID++) {
      Document doc = new Document();
      for (String field: fields) {
        doc.add(new Field(
                field,
                getRandomString(random, CHARS, 1, fieldContentLength) + docID,
                Field.Store.NO, Field.Index.NOT_ANALYZED));
      }
      doc.add(new Field("num", Integer.toString(docID),
          Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("all", "all",
          Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
      writer.addDocument(doc);
      if (maxSegments > 1 && docID == docCount / maxSegments) {
        writer.commit(); // We want multiple segments
      }
    }
    if (maxSegments != -1) {
      writer.optimize(maxSegments);
    }
    writer.close();
    System.out.println(String.format(
        "Created %d document index with %d fields with average " +
            "term length %d and total size %s in a maximum of " +
            "%d segments in %s",
        docCount, fields.size(), fieldContentLength / 2,
        ExposedPOC.readableSize(ExposedPOC.calculateSize(location)),
        maxSegments,
        ExposedSegmentReader.nsToString(System.nanoTime() - startTime)));
  }

  private StringBuffer buffer = new StringBuffer(100);
  private synchronized String getRandomString(
          Random random, char[] chars, int minLength, int maxLength) {
    int length = minLength == maxLength ? minLength :
            random.nextInt(maxLength-minLength+1) + minLength;
    buffer.setLength(0);
    for (int i = 0 ; i < length ; i++) {
      buffer.append(chars[random.nextInt(chars.length)]);
    }
    return buffer.toString().trim(); // Discard leading and trailing spaces
  }

                                     /*
  public void testCount() throws Exception {
    ExposedReader exposed = createExposedSegment();
    assertEquals("There should be the right number of terms",
            DOCCOUNT, exposed.getTermCount("a"));
  }

  public void testPosition() throws Exception {
    ExposedReader exposed = createExposedSegment();
    assertEquals("The base for field a should be correct",
            0, exposed.getBase("a"));
    assertEquals("The base for field a should be correct",
            DOCCOUNT, exposed.getBase("b"));
  }

  public void testSortedTerms() throws Exception {
    final String FIELD = "b";
    ExposedSegmentReader exposed = createExposedSegment();
    long startTime = System.nanoTime();
    PackedInts.Reader orderedTerms = exposed.getSortedTerms(
                    Collator.getInstance(new Locale("da")), FIELD);
    System.out.println(String.format(
            "Extracted Term order for %d terms in %sms",
            orderedTerms.size(), (System.nanoTime() - startTime) / 1000000.0));
    for (int i = 0 ; i < orderedTerms.size() && i < 10 ; i++) {
      Term term = exposed.getTerm((int)orderedTerms.get(i));
      System.out.print(String.format(
              "term %s(%d) has termPos %d = %s with docIDs",
              FIELD, i, orderedTerms.get(i), term.text()));
      TermDocs termDocs = exposed.termDocs(term);
      while (termDocs.next()) {
        System.out.print(" " + termDocs.doc());
      }
      System.out.println("");
    }
  }

  public void testSortedTermsPerformance() throws Exception {
    final String FIELD = "b";
    final int TERM_LENGTH = 40;
    final int[] DOC_COUNTS = new int[]{
            1000000};
    //100, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 50000000};
    for (int docCount: DOC_COUNTS) {
      deleteIndex();
      createIndex(INDEX_LOCATION, docCount,
          Arrays.asList("a", "b", "c", "d", "e", "f", "g"), TERM_LENGTH);

      measureExposedSort(INDEX_LOCATION, FIELD, TERM_LENGTH);
      System.out.println("\nFor comparison, we load the Strings into memory " +
          "and sort them there:");
      measureFlatSort(INDEX_LOCATION, FIELD, docCount);
//      measureStringIndex(INDEX_LOCATION, FIELD, docCount);
      System.out.println("");
    }
                                            
  }

  private void measureExposedSort(
          File location, String field, int termLength) throws Exception {
    IndexReader reader = openIndex(location);
    System.out.println("Opened index from " + location+ ". Heap: "
            + ExposedPOC.getHeap());
    ExposedSegmentReader exposed = new ExposedSegmentReader(
            SegmentReader.getOnlySegmentReader(reader));

    long startTimeTerm = System.nanoTime();
    PackedInts.Reader orderedTerms = exposed.getSortedTerms(
            Collator.getInstance(new Locale("da")), field);
    long termTime = System.nanoTime() - startTimeTerm;

    long startTimeDoc = System.nanoTime();
    final PackedInts.Reader orderedDocs =
            exposed.getSortedDocIDs(field, orderedTerms);
    long docTime = System.nanoTime() - startTimeDoc;

    System.out.println(String.format(
        "Got ordered docIDs in %s (%s total), sorted %d docIDs in " +
            "%s %s %s. Heap: %s (orderedTerms: %s, " +
            "orderedDocs: %s).",
        ExposedSegmentReader.nsToString(docTime),
        ExposedSegmentReader.nsToString(termTime + docTime),
        orderedDocs.size(),
        ExposedPOC.measureSortTime(orderedDocs),
        ExposedPOC.measureSortTime(orderedDocs),
        ExposedPOC.measureSortTime(orderedDocs),
        ExposedPOC.getHeap(),
        ExposedPOC.readableSize(ExposedPOC.footprint(orderedTerms)),
        ExposedPOC.readableSize(ExposedPOC.footprint(orderedDocs))));

    exposed.close();
  }

  private void measureFlatSort(
          File location, String field, int docCount) throws Exception {
    IndexReader reader = openIndex(location);

    long startTimeTerm = System.nanoTime();
    String[] terms = FieldCache.DEFAULT.getStrings(reader, field);
    System.out.println(String.format(
            "Loaded %d terms by FieldCacheDEFAULT.getStrings in %s. Heap: %s",
            terms.length, ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTimeTerm),
            ExposedPOC.getHeap()));

    long startTimeSort = System.nanoTime();
    Collator collator = Collator.getInstance(new Locale("da"));
    Arrays.sort(terms, collator);
    System.out.println(String.format(
        "Sorted (Arrays.sort with collator) the array of %d terms in %s",
            terms.length, ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTimeSort)));
    reader.close();
  }

  private void measureStringIndex(
          File location, String field, int docCount) throws Exception {
    IndexReader reader = openIndex(location);
    long startTimeIndex = System.nanoTime();
    FieldCache.StringIndex index =
            FieldCache.DEFAULT.getStringIndex(reader, field);
    System.out.println(String.format(
            "Got StringIndex with %d terms in %s. Heap: %s",
            index.order.length, ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTimeIndex),
            ExposedPOC.getHeap()));
  }

  private void measureStringFieldComparator(
          File location, String field, int docCount) throws Exception {
    IndexReader reader = openIndex(location);
    long startTimeComparator = System.nanoTime();
    SortField sortField = new SortField(field, new Locale("da"));
    FieldComparator comparator = sortField.getComparator(100, 0);
    comparator.setNextReader(reader, 0);
    System.out.println(String.format(
         "Initialized FieldComparator with locale for %d terms in %s. Heap: %s",
            docCount, ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTimeComparator),
            ExposedPOC.getHeap()));
  }



  private IndexReader openIndex(File location) throws Exception {
    IndexReader reader = IndexReader.open(FSDirectory.open(location), true);
    TermEnum terms = reader.terms(new Term("b", "")); // Warming
    while (terms.next()) {
      // Skip
    }
    return reader;
  }

  private long getTermCacheOverhead(
          ExposedSegmentReader exposed, int averageTermSize) {
    return exposed.getSortCacheSize() * (48 + 2 * averageTermSize);
  }

  public void testSortedDocuments() throws Exception {
    final String FIELD = "b";
    ExposedSegmentReader exposed = createExposedSegment();
    PackedInts.Reader orderedTerms = exposed.getSortedTerms(
                    Collator.getInstance(new Locale("da")), FIELD);

    long startTimeDoc = System.nanoTime();
    PackedInts.Reader orderedDocs =
            exposed.getSortedDocIDs(FIELD, orderedTerms);
    System.out.println("Got ordered docIDs in " 
            + (System.nanoTime() - startTimeDoc) / 1000000 + "ms");

    for (int i = 0 ; i < orderedDocs.size() && i < 10 ; i++) {
      System.out.println(String.format(
              "document %d has position %d and Term %s",
              i, orderedDocs.get(i),
              exposed.getTerm((int)orderedTerms.get((int)orderedDocs.get(i)))));
    }
  }

  public void testCollatorKeySize() {
    long KEYS = 20000;
    long keySize = 0;
    Random random = new Random(87);
    Collator collator = Collator.getInstance(new Locale("da"));
    for (int i = 0 ; i < KEYS ; i++) {
      keySize += collator.getCollationKey(getRandomString(
          random, CHARS, 30, 30)).toByteArray().length;
    }
    System.out.println("Average size: " + keySize * 1.0 / KEYS);
  }
          */

}
