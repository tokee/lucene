package org.apache.lucene.index;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.packed.PackedInts;

import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.*;

public class TestExposed extends LuceneTestCase {

  static final char[] CHARS = // Used for random content
          ("abcdefghijklmnopqrstuvwxyzæøåABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ" +
                  "1234567890      ").toCharArray();
  static final File INDEX_LOCATION =
//          new File(System.getProperty("java.io.tmpdir"), "exposed_index");
//          new File("/home/te/projects/lucene/exposed_index");
          new File("/mnt/bulk/exposed_index");
  public static final int DOCCOUNT = 10000;

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

  private ExposedSegmentReader createExposed() throws IOException {
    createIndex(INDEX_LOCATION, DOCCOUNT, Arrays.asList("a", "b"), 20);
    IndexReader reader = IndexReader.open(
            FSDirectory.open(INDEX_LOCATION), true);
    return new ExposedSegmentReader(
            SegmentReader.getOnlySegmentReader(reader));

  }

  public void testCount() throws Exception {
    ExposedReader exposed = createExposed();
    assertEquals("There should be the right number of terms",
            DOCCOUNT, exposed.getTermCount("a"));
  }

  public void testPosition() throws Exception {
    ExposedReader exposed = createExposed();
    assertEquals("The base for field a should be correct",
            0, exposed.getBase("a"));
    assertEquals("The base for field a should be correct",
            DOCCOUNT, exposed.getBase("b"));
  }

  public void testSortedTerms() throws Exception {
    final String FIELD = "b";
    ExposedSegmentReader exposed = createExposed();
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
      measureFlatSort(INDEX_LOCATION, FIELD, docCount);
      measureStringIndex(INDEX_LOCATION, FIELD, docCount);
      System.out.println("");
    }
                                            
  }

  private void measureExposedSort(
          File location, String field, int termLength) throws Exception {
    IndexReader reader = openIndex(location);
    System.out.println("Opened index from " + location+ ". Heap used: "
            + getHeap());
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
                    "%s %s %s. Heap usage: %s (orderedTerms: %s, " +
                    "orderedDocs: %s). Temporary build overhead: %s",
            ExposedSegmentReader.nsToString(docTime),
            ExposedSegmentReader.nsToString(termTime + docTime),
            orderedDocs.size(),
            measureSortTime(orderedDocs),
            measureSortTime(orderedDocs),
            measureSortTime(orderedDocs),
            getHeap(), readableSize(orderedTerms.ramBytesUsed()),
            readableSize(orderedDocs.ramBytesUsed()),
            readableSize(orderedDocs.size() * 4 * 2
                    + getTermCacheOverhead(exposed, termLength / 2))));

    exposed.close();
  }

  private void measureFlatSort(
          File location, String field, int docCount) throws Exception {
    IndexReader reader = openIndex(location);

    long startTimeTerm = System.nanoTime();
    String[] terms = FieldCache.DEFAULT.getStrings(reader, field);
    System.out.println(String.format(
            "Loaded %d terms by FieldCache in %s. Heap: %s",
            terms.length, ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTimeTerm),
            getHeap()));

    long startTimeSort = System.nanoTime();
    Collator collator = Collator.getInstance(new Locale("da"));
    Arrays.sort(terms, collator);
    System.out.println(String.format("Sorted an array with the %d terms in %s",
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
            getHeap()));
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
            getHeap()));
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

  private String getHeap() throws InterruptedException { // Calls gc() first
    for (int i = 0 ; i < 4 ; i++) {
      System.gc();
      Thread.sleep(10);
    }
    return readableSize(Runtime.getRuntime().totalMemory()
            - Runtime.getRuntime().freeMemory());
  }

  private String measureSortTime(final PackedInts.Reader orderedDocs) {
    Integer[] allDocIDS = new Integer[orderedDocs.size()];
    for (int i = 0 ; i < allDocIDS.length ; i++) {
      allDocIDS[i] = i;
    }
    long startTimeSort = System.nanoTime();
    Arrays.sort(allDocIDS, new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
        return (int)(orderedDocs.get(o1) - orderedDocs.get(o2));
      }
    });
    return ExposedSegmentReader.nsToString(
            System.nanoTime() - startTimeSort);
  }

  public void testSortedDocuments() throws Exception {
    final String FIELD = "b";
    ExposedSegmentReader exposed = createExposed();
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

  private void createIndex(File location, int docCount,
                           List<String> fields, int fieldContentLength)
                                                            throws IOException {
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
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();
    System.out.println(String.format(
            "Created %d document optimized index with %d fields with average " +
                    "term length %d and total size %s in %s",
            docCount, fields.size(), fieldContentLength / 2,
            readableSize(calculateSize(location)),
            ExposedSegmentReader.nsToString(
                    System.nanoTime() - startTime)));
  }

  private String readableSize(long size) {
    return size > 2 * 1048576 ?
            size / 1048576 + "MB" :
            size > 2 * 1024 ?
                    size / 1024 + "KB" :
                    size + "bytes";
  }

  private long calculateSize(File file) {
    long size = 0;
    if (file.isDirectory()) {
      for (File sub: file.listFiles()) {
        size += calculateSize(sub);
      }
    } else {
      size += file.length();
    }
    return size;
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
    return buffer.toString();
  }



}
/*
/usr/lib/jvm/java-6-sun/bin/java -Xmx1292m -Didea.launcher.port=7552 -Didea.launcher.bin.path=/home/te/bin/idea9/bin -Dfile.encoding=UTF-8 -classpath /home/te/bin/idea9/lib/idea_rt.jar:/home/te/bin/idea9/plugins/junit/lib/junit-rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/jsse.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/plugin.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/alt-rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/deploy.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/javaws.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/jce.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/charsets.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/resources.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/management-agent.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/dnsns.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-6-sun/jre/lib/charsets.jar:/usr/lib/jvm/java-6-sun/jre/lib/jsse.jar:/usr/lib/jvm/java-6-sun/jre/lib/management-agent.jar:/usr/lib/jvm/java-6-sun/jre/lib/rt.jar:/usr/lib/jvm/java-6-sun/jre/lib/resources.jar:/usr/lib/jvm/java-6-sun/jre/lib/jce.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/dnsns.jar:/home/te/projects/lucene/out/test/Lucene:/home/te/projects/lucene/out/production/Lucene:/home/te/projects/lucene/lib/junit-4.7.jar:/home/te/projects/lucene/lib/servlet-api-2.4.jar:/home/te/projects/lucene/out/test/Remote:/home/te/projects/lucene/out/production/Remote:/home/te/projects/lucene/out/test/Misc:/home/te/projects/lucene/out/production/Misc:/home/te/projects/lucene/out/test/Queries:/home/te/projects/lucene/out/production/Queries com.intellij.rt.execution.application.AppMain com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 org.apache.lucene.index.TestExposed,testSortedTermsPerformance
Created 100 document optimized index with 7 fields with average term length 20 and total size 23KB in 209ms
Opened index from /tmp/exposed_index. Heap used: 630KB
Sorted 100 Terms in 12ms out of which 778846ns (6%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 710 requests with 100 (14%) misses
Got ordered docIDs in 12ms (108ms total), sorted 100 docIDs in 569521ns 285675ns 278133ns. Heap usage: 1144KB (orderedTerms: 116bytes, orderedDocs: 116bytes). Temporary build overhead: 1719KB
Loaded 100 terms by FieldCache in 3ms. Heap: 1170KB
Sorted an array with the 100 terms in 25ms
Got StringIndex with 100 terms in 2ms. Heap: 1170KB

Created 1000 document optimized index with 7 fields with average term length 20 and total size 240KB in 477ms
Opened index from /tmp/exposed_index. Heap used: 2MB
Sorted 1000 Terms in 245ms out of which 7ms (2%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 11668 requests with 1000 (8%) misses
Got ordered docIDs in 133ms (384ms total), sorted 1000 docIDs in 7ms 10ms 11ms. Heap usage: 2MB (orderedTerms: 1284bytes, orderedDocs: 1284bytes). Temporary build overhead: 1726KB
Loaded 1000 terms by FieldCache in 4ms. Heap: 2MB
Sorted an array with the 1000 terms in 76ms
Got StringIndex with 1000 terms in 29ms. Heap: 2MB

Created 5000 document optimized index with 7 fields with average term length 20 and total size 1223KB in 988ms
Opened index from /tmp/exposed_index. Heap used: 6MB
Sorted 5000 Terms in 203ms out of which 17ms (8%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 80674 requests with 5000 (6%) misses
Got ordered docIDs in 248ms (471ms total), sorted 5000 docIDs in 20ms 22ms 20ms. Heap usage: 6MB (orderedTerms: 7KB, orderedDocs: 7KB). Temporary build overhead: 1757KB
Loaded 5000 terms by FieldCache in 14ms. Heap: 6MB
Sorted an array with the 5000 terms in 54ms
Got StringIndex with 5000 terms in 5ms. Heap: 6MB

Created 10000 document optimized index with 7 fields with average term length 20 and total size 2MB in 441ms
Opened index from /tmp/exposed_index. Heap used: 12MB
Sorted 10000 Terms in 164ms out of which 19ms (11%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 172844 requests with 10000 (5%) misses
Got ordered docIDs in 267ms (455ms total), sorted 10000 docIDs in 4ms 4ms 4ms. Heap usage: 12MB (orderedTerms: 17KB, orderedDocs: 17KB). Temporary build overhead: 1796KB
Loaded 10000 terms by FieldCache in 10ms. Heap: 12MB
Sorted an array with the 10000 terms in 116ms
Got StringIndex with 10000 terms in 9ms. Heap: 12MB

Created 50000 document optimized index with 7 fields with average term length 20 and total size 12MB in 1944ms
Opened index from /tmp/exposed_index. Heap used: 18MB
Sorted 50000 Terms in 1758ms out of which 1112ms (63%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 1033380 requests with 136446 (13%) misses
Got ordered docIDs in 759ms (2570ms total), sorted 50000 docIDs in 44ms 20ms 18ms. Heap usage: 18MB (orderedTerms: 97KB, orderedDocs: 97KB). Temporary build overhead: 2MB
Loaded 50000 terms by FieldCache in 50ms. Heap: 24MB
Sorted an array with the 50000 terms in 662ms
Got StringIndex with 50000 terms in 52ms. Heap: 25MB

Created 100000 document optimized index with 7 fields with average term length 20 and total size 24MB in 3238ms
Opened index from /tmp/exposed_index. Heap used: 25MB
Sorted 100000 Terms in 4520ms out of which 2971ms (65%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 2266240 requests with 326655 (14%) misses
Got ordered docIDs in 1455ms (6035ms total), sorted 100000 docIDs in 63ms 62ms 61ms. Heap usage: 26MB (orderedTerms: 207KB, orderedDocs: 207KB). Temporary build overhead: 2MB
Loaded 100000 terms by FieldCache in 32ms. Heap: 31MB
Sorted an array with the 100000 terms in 1299ms
Got StringIndex with 100000 terms in 34ms. Heap: 32MB

Created 500000 document optimized index with 7 fields with average term length 20 and total size 125MB in 0:18 min
Opened index from /tmp/exposed_index. Heap used: 22MB
Sorted 500000 Terms in 0:35 min out of which 0:26 min (74%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 13015128 requests with 2533011 (19%) misses
Got ordered docIDs in 7639ms (0:42 min total), sorted 500000 docIDs in 413ms 420ms 424ms. Heap usage: 25MB (orderedTerms: 1159KB, orderedDocs: 1159KB). Temporary build overhead: 5MB
Loaded 500000 terms by FieldCache in 169ms. Heap: 85MB
Sorted an array with the 500000 terms in 8963ms
Got StringIndex with 500000 terms in 178ms. Heap: 87MB

Created 1000000 document optimized index with 7 fields with average term length 20 and total size 251MB in 0:35 min
Opened index from /tmp/exposed_index. Heap used: 29MB
Sorted 999999 Terms in 1:22 min out of which 1:03 min (77%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 27330816 requests with 5826096 (21%) misses
Got ordered docIDs in 0:15 min (1:38 min total), sorted 1000000 docIDs in 942ms 960ms 951ms. Heap usage: 34MB (orderedTerms: 2MB, orderedDocs: 2MB). Temporary build overhead: 9MB
Loaded 1000000 terms by FieldCache in 348ms. Heap: 156MB
Sorted an array with the 1000000 terms in 0:18 min
Got StringIndex with 1000000 terms in 339ms. Heap: 160MB

Created 5000000 document optimized index with 7 fields with average term length 20 and total size 1298MB in 3:07 min
Opened index from /home/te/projects/lucene/exposed_index. Heap used: 72MB
Sorted 4999999 Terms in 10:41 min out of which 8:09 min (76%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 155213328 requests with 37782392 (24%) misses
Got ordered docIDs in 1:28 min (12:13 min total), sorted 5000000 docIDs in 6540ms 6374ms 6596ms. Heap usage: 100MB (orderedTerms: 13MB, orderedDocs: 13MB). Temporary build overhead: 39MB
Loaded 5000000 terms by FieldCache in 4710ms. Heap: 718MB
Sorted an array with the 5000000 terms in 17:56 min <- Most probably due to memory trouble
Got StringIndex with 5000000 terms in 5155ms. Heap: 737MB


/usr/lib/jvm/java-6-sun/bin/java -Xmx4292m -Didea.launcher.port=7560 -Didea.launcher.bin.path=/home/te/bin/idea9/bin -Dfile.encoding=UTF-8 -classpath /home/te/bin/idea9/lib/idea_rt.jar:/home/te/bin/idea9/plugins/junit/lib/junit-rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/jsse.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/plugin.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/alt-rt.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/deploy.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/javaws.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/jce.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/charsets.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/resources.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/management-agent.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/dnsns.jar:/usr/lib/jvm/java-6-sun-1.6.0.15/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-6-sun/jre/lib/charsets.jar:/usr/lib/jvm/java-6-sun/jre/lib/jsse.jar:/usr/lib/jvm/java-6-sun/jre/lib/management-agent.jar:/usr/lib/jvm/java-6-sun/jre/lib/rt.jar:/usr/lib/jvm/java-6-sun/jre/lib/resources.jar:/usr/lib/jvm/java-6-sun/jre/lib/jce.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-6-sun/jre/lib/ext/dnsns.jar:/home/te/projects/lucene/out/test/Lucene:/home/te/projects/lucene/out/production/Lucene:/home/te/projects/lucene/lib/junit-4.7.jar:/home/te/projects/lucene/lib/servlet-api-2.4.jar:/home/te/projects/lucene/out/test/Remote:/home/te/projects/lucene/out/production/Remote:/home/te/projects/lucene/out/test/Misc:/home/te/projects/lucene/out/production/Misc:/home/te/projects/lucene/out/test/Queries:/home/te/projects/lucene/out/production/Queries com.intellij.rt.execution.application.AppMain com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 org.apache.lucene.index.TestExposed,testSortedTermsPerformance
Created 10000000 document optimized index with 7 fields with average term length 20 and total size 2600MB in 7:00 min
Opened index from /home/te/projects/lucene/exposed_index. Heap used: 113MB
Sorted 9999999 Terms in 23:14 min out of which 17:23 min (74%) was lookups and 0ns (0%) was collation key creation. The cache (20000 terms) got 328840118 requests with 83147925 (25%) misses
Got ordered docIDs in 2:45 min (26:07 min total), sorted 10000000 docIDs in 0:13 min 0:12 min 0:12 min. Heap usage: 171MB (orderedTerms: 28MB, orderedDocs: 28MB). Temporary build overhead: 77MB
Loaded 10000000 terms by FieldCache in 0:26 min. Heap: 1351MB
Sorted an array with the 10000000 terms in 3:39 min
Got StringIndex with 10000000 terms in 0:11 min. Heap: 1390MB


*/