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
import java.text.Collator;
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
    String type = args[0];
    File location = new File(args[1]);
    String field = args[2];
    Locale locale = new Locale(args[3]);
    String defaultField = args[4];

    if ("expose".equals(type)) {
      expose(location, field, locale, defaultField);
    } else if ("default".equals(type)) {
      defaultSort(location, field, locale, defaultField);
    } else {
      System.out.println("Unknown action '" + type + "'\n");
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

  private static void expose(
      File location, String field, Locale locale, String defaultField)
      throws IOException, InterruptedException, ParseException {
    System.out.println(String.format(
        "Exposing index at '%s' with sort on field %s with locale %s. Heap: %s",
        location, field, locale, getHeap()));

    IndexReader reader = IndexReader.open(
        FSDirectory.open(location), true);
    System.out.println(String.format(
        "Opened index of size %s from %s. Heap: %s",
        readableSize(calculateSize(location)), location, getHeap()));
    ExposedSegmentReader exposed = new ExposedSegmentReader(
            SegmentReader.getOnlySegmentReader(reader));
    System.out.println(String.format(
        "Wrapped SegmentReader in ExposedSegmentReader. Sorting terms for " +
            "field %s with locale %s... Heap: %s",
        field, locale, getHeap()));

    long startTimeTerm = System.nanoTime();
    PackedInts.Reader orderedTerms = exposed.getSortedTerms(
            Collator.getInstance(locale), field);
    long termTime = System.nanoTime() - startTimeTerm;

    System.out.println("Ordering docIDs aligned to sorted terms...");

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
            measureSortTime(orderedDocs),
            measureSortTime(orderedDocs),
            measureSortTime(orderedDocs),
            getHeap(), readableSize(footprint(orderedTerms)),
            readableSize(footprint(orderedDocs))));

    IndexSearcher searcher = new IndexSearcher(reader);
    Sort exposedSort = new Sort(new SortField(field,
        new ExposedComparatorSource(exposed, orderedTerms, orderedDocs)));

    System.out.println(String.format(
        "\nFinished initializing exposed structures for field %s.\n"
        + "Write standard Lucene queries to experiment with sorting speed.\n"
        + "The StandardAnalyser will be used and there is no default field.\n"
        + "Finish with 'EXIT'", field));
    String query = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    QueryParser qp = new QueryParser(
        Version.LUCENE_31, defaultField,
        new StandardAnalyzer(Version.LUCENE_31));

    while (true) {
      System.out.print("Query: ");
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
        TopFieldDocs topDocs = searcher.search(q, null, 20, exposedSort);
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
              "Doc #%d has %s", docID, exposed.getTerm(
                  (int)orderedTerms.get((int)orderedDocs.get(docID)))));
        }
        System.out.println("Extracting terms for the search result took "
            + ExposedSegmentReader.nsToString(
            System.nanoTime() - startTimeDisplay) + ". Heap: " + getHeap());
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
      }
    }
    System.out.println("\nThank you for playing. Please come back.");
  }

  private static void defaultSort(
      File location, String field, Locale locale, String defaultField)
                                      throws IOException, InterruptedException {
    System.out.println(String.format(
        "Normal open of index at '%s' with sort on field %s with locale %s. " +
            "Heap: %s",
        location, field, locale, getHeap()));

    IndexReader reader = IndexReader.open(FSDirectory.open(location), true);
    System.out.println(String.format(
        "Opened index of size %s from %s. Heap: %s",
        readableSize(calculateSize(location)), location, getHeap()));


    IndexSearcher searcher = new IndexSearcher(reader);
    Sort normalSort = new Sort(new SortField(field, locale));

    System.out.println(String.format(
        "\nFinished initializing exposed structures for field %s.\n"
        + "Write standard Lucene queries to experiment with sorting speed.\n"
        + "The StandardAnalyser will be used and there is no default field.\n"
        + "Finish with 'EXIT'", field));
    String query = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    QueryParser qp = new QueryParser(
        Version.LUCENE_31, defaultField,
        new StandardAnalyzer(Version.LUCENE_31));

    while (true) {
      System.out.print("Query: ");
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
        TopFieldDocs topDocs = searcher.search(q, null, 20, normalSort);
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
          System.out.println(String.format("Doc #%d", docID));
        }
        System.out.println("Extracting terms for the search result took "
            + ExposedSegmentReader.nsToString(
            System.nanoTime() - startTimeDisplay) + ". Heap: " + getHeap());
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
      }
    }
    System.out.println("\nThank you for playing. Please come back.");
  }



  static class ExposedComparatorSource extends FieldComparatorSource {
    ExposedSegmentReader exposedReader;
    private PackedInts.Reader orderedTerms;
    private PackedInts.Reader orderedDocs;

    private ExposedComparatorSource(ExposedSegmentReader exposedReader,
                                    PackedInts.Reader orderedTerms,
                                    PackedInts.Reader orderedDocs) {
      this.exposedReader = exposedReader;
      this.orderedTerms = orderedTerms;
      this.orderedDocs = orderedDocs;
    }

    @Override
    public FieldComparator newComparator(
        String fieldname, int numHits, int sortPos, boolean reversed)
                                                            throws IOException {
      return new ExposedComparator(
          orderedDocs,fieldname, numHits, sortPos, reversed);
    }
  }

  static class ExposedComparator extends FieldComparator {
    private PackedInts.Reader orderedDocs;
    private String fieldname;
    private int numHits;
    private int sortPos;
    private boolean reversed;
    private int factor = 1;

    private int[] order;
    private int bottom;

    public ExposedComparator(
        PackedInts.Reader orderedDocs,
        String fieldname, int numHits, int sortPos, boolean reversed) {
      this.orderedDocs = orderedDocs;
      this.fieldname = fieldname;
      this.numHits = numHits;
      this.sortPos = sortPos;
      this.reversed = reversed;
      if (reversed) {
        factor = -1;
      }
      order = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return (factor * (order[slot1] - order[slot2]));
    }

    @Override
    public void setBottom(int slot) {
      bottom = order[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return (int)(factor * (bottom - orderedDocs.get(doc)));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      order[slot] = (int)orderedDocs.get(doc);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase)
                                                            throws IOException {
      // Ignore as we only support a single reader in this proof of concept
    }

    @Override
    public Comparable<?> value(int slot) {
      return order[slot];
    }
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
            + "ExposedPOC expose /mnt/bulk/40GB_index author da"
            + "\n"
            + "If the index is not optimized, it can be done with\n"
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
