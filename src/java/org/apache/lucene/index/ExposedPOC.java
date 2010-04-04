package org.apache.lucene.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedInts;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

public class ExposedPOC {
  private static final double MAX_HITS = 20;

  public static void main(String[] args)
      throws IOException, InterruptedException, ParseException {
    if (args.length == 2 && "optimize".equals(args[0])) {
      optimize(new File(args[1]));
      return;
    }
    if (args.length != 5) {
      System.err.println("Need 5 arguments, got " + args.length + "\n");
      usage();
      return;
    }
    String method = args[0];
    File location = new File(args[1]);
    String field = args[2];
    Locale locale = new Locale(args[3]);
    String defaultField = args[4];

    try {
      shell(method, location, field, locale, defaultField);
    } catch (Exception e) {
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
      usage();
    }
  }

  private static void optimize(File location) throws IOException {
    System.out.println("Optimizing " + location + "...");
    long startTimeOptimize = System.nanoTime();
    IndexWriter writer = new IndexWriter(FSDirectory.open(location),
        new IndexWriterConfig(Version.LUCENE_31,
            new StandardAnalyzer(Version.LUCENE_31)));
    writer.optimize(1);
    System.out.println("Optimized index in " + ExposedSegmentReader.nsToString(
        System.nanoTime() - startTimeOptimize));
    writer.close(true);
  }

  private static void shell(
      String method, File location, String field, Locale locale,
      String defaultField)
      throws IOException, InterruptedException, ParseException {
    System.out.println(String.format(
        "Testing sorted search for index at '%s' with sort on field %s with " +
            "locale %s, using sort-method %s. Heap: %s",
        location, field, locale, method, getHeap()));

    IndexReader reader = IndexReader.open(FSDirectory.open(location), true);
    System.out.println(String.format(
        "Opened index of size %s from %s. Heap: %s",
        readableSize(calculateSize(location)), location, getHeap()));

/*    System.out.println(String.format(
        "Creating %s Sort for field %s with locale %s... Heap: %s",
        method, field, locale, getHeap()));
  */
    long startTimeSort = System.nanoTime();
    Sort sort;
    if ("exposed".equals(method)) {
      ExposedFieldComparatorSource exposedFCS =
          new ExposedFieldComparatorSource(reader, new Locale("da"));
      sort = new Sort(new SortField(field, exposedFCS));
    } else if ("default".equals(method)) {
      sort = new Sort(new SortField(field, locale));
    } else {
      throw new IllegalArgumentException(
          "The sort method " + method + " is unsupported");
    }
    long sortTime = System.nanoTime() - startTimeSort;

    System.out.println(String.format(
        "Created %s Sort for field %s in %s. Heap: %s",
        method, field, ExposedSegmentReader.nsToString(sortTime), getHeap()));

    IndexSearcher searcher = new IndexSearcher(reader);

    System.out.println(String.format(
        "\nFinished initializing %s structures for field %s.\n"
        + "Write standard Lucene queries to experiment with sorting speed.\n"
        + "The StandardAnalyser will be used and the default field is %s.\n"
        + "Finish with 'EXIT'\n", method, field, defaultField));
    String query;
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    QueryParser qp = new QueryParser(
        Version.LUCENE_31, defaultField,
        new StandardAnalyzer(Version.LUCENE_31));

    boolean first = true;
    while (true) {
      if (first) {
        System.out.print("Query (" + method + " sort, first search might take " +
            "a while): ");
        first = false;
      } else {
        System.out.print("Query (" + method + " sort): ");
      }
      query = br.readLine();
      if ("".equals(query)) {
        continue;
      }
      if ("EXIT".equals(query)) {
        break;
      }
      try {
        long startTimeQuery = System.nanoTime();
        Query q = qp.parse(query);
        long queryTime = System.nanoTime() - startTimeQuery;

        long startTimeSearch = System.nanoTime();
        TopFieldDocs topDocs = searcher.search(
            q.weight(searcher), null, 20, sort, true);
        long searchTime = System.nanoTime() - startTimeSearch;
        System.out.println(String.format(
            "The search for '%s' got %d hits in %s (+ %s for query parsing). "
                + "Showing %d hits. Heap: %s",
            query, topDocs.totalHits,
            ExposedSegmentReader.nsToString(searchTime),
            ExposedSegmentReader.nsToString(queryTime),
            (int)Math.min(topDocs.totalHits, MAX_HITS), getHeap()));
        long startTimeDisplay = System.nanoTime();
        for (int i = 0 ; i < Math.min(topDocs.totalHits, MAX_HITS) ; i++) {
          int docID = topDocs.scoreDocs[i].doc;
          System.out.println(String.format(
              "Hit #%d was doc #%d with %s:%s",
              i, docID, field, ((FieldDoc)topDocs.scoreDocs[i]).fields[0]));
        }
        System.out.println("Displaying the search result took "
            + ExposedSegmentReader.nsToString(
            System.nanoTime() - startTimeDisplay) + ". Heap: " + getHeap());
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
      }
    }
    System.out.println("\nThank you for playing. Please come back.");
  }

  private static void usage() {
    System.out.println(
        "Usage: ExposedPOC expose|default <index> <sortField> <locale>" +
            " <defaultField>\n"
            + "expose:         Uses the expose sorter\n"
            + "default:        Uses the default sorter\n"
            + "<index>:        The location of an optimized Lucene index\n"
            + "<sortField>:    The field to use for sorting\n"
            + "<locale>:       The locale to use for sorting\n"
            + "<defaultField>: The field to search when no explicit field is " +
            "given\n"
            + "\n"
            + "Example:\n"
            + "ExposedPOC expose /mnt/bulk/40GB_index author da freetext"
            + "\n"
            + "If the index is is to be optimized, it can be done with\n"
            + "ExposedPOC optimize <index>\n"
            + "\n"
            + "Note: This is a proof of concept. Expect glitches!"
    );
  }

  static String getHeap() throws InterruptedException { // Calls gc() first
    for (int i = 0 ; i < 4 ; i++) {
      System.gc();
      Thread.sleep(10);
    }
    return readableSize(Runtime.getRuntime().totalMemory()
            - Runtime.getRuntime().freeMemory());
  }

  static String readableSize(long size) {
    return size > 2 * 1048576 ?
            size / 1048576 + "MB" :
            size > 2 * 1024 ?
                    size / 1024 + "KB" :
                    size + "bytes";
  }

  static long calculateSize(File file) {
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

  static String measureSortTime(final PackedInts.Reader orderedDocs) {
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

  static long footprint(PackedInts.Reader values) {
    return values.getBitsPerValue() * values.size() / 8;
  }

}
