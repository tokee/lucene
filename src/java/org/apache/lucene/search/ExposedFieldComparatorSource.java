package org.apache.lucene.search;

import org.apache.lucene.index.ExposedReader;
import org.apache.lucene.index.ExposedSegmentReader;
import org.apache.lucene.index.ExposedUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.text.Collator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Provides collator- or locale-based low memory sort by using the Exposed
 * framework. Currently only DirectoryReader and SegmentReaders are supported.
 * </p><p>
 * The comparator source needs to be discarded when the index is (re)opened.
 */
// Highly experimental!
public class ExposedFieldComparatorSource extends FieldComparatorSource {
  private final ExposedReader reader;
  private final Comparator<Object> comparator;
  private final String persistenceKeyPrefix;
  private final Map<String, ExposedUtil.SortArrays> cache =
      new HashMap<String, ExposedUtil.SortArrays>(5);

  public ExposedFieldComparatorSource(
     IndexReader reader, String persistenceKey, Comparator<Object> comparator) {
    if (!(reader instanceof ExposedReader)) {
      throw new UnsupportedOperationException(
          "The provided IndexReader is not an ExposedReader. " +
              "Currently only DirectoryReader and SegmentReader are Exposed");
    }
    this.reader = (ExposedReader)reader;
    persistenceKeyPrefix = persistenceKey;
    this.comparator = comparator;
  }

  public ExposedFieldComparatorSource(IndexReader reader, Locale locale) {
    this(reader, locale == null ? "null" : locale.toString(),
        Collator.getInstance(locale));
  }

  @Override
  public FieldComparator newComparator(
      String fieldname, int numHits, int sortPos, boolean reversed)
                                                            throws IOException {
    return new ExposedFieldComparator(
        reader, fieldname, numHits, sortPos, reversed);
  }

  private class ExposedFieldComparator extends FieldComparator {
    private final PackedInts.Reader docOrder;
    private final PackedInts.Reader termOrder;

    private String fieldname;
    private int numHits;
    private int sortPos;
    private boolean reversed;
    private int factor = 1;

    private int[] order; // docID TODO: Consider using termOrder index instead
    private int bottom;  // docID

    public ExposedFieldComparator(
        ExposedReader reader, String fieldname, int numHits, int sortPos,
        boolean reversed) throws IOException {
      final String key = persistenceKeyPrefix + "_" + fieldname;
      ExposedUtil.SortArrays sortArrays = cache.get(key);
      if (sortArrays == null) {
        long startTime = System.nanoTime();
        sortArrays = ExposedUtil.getSortArrays(
            reader, key, fieldname, comparator);
        System.out.println(
            "Calculated exposed sort structures for field " + fieldname 
                + " in " + (System.nanoTime() - startTime) / 1000000 + "ms");
        cache.put(key, sortArrays);
      }
      docOrder = sortArrays.docOrder;
      termOrder = sortArrays.termOrder;
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
      return (int)(factor *
          (docOrder.get(order[slot1]) - docOrder.get(order[slot2])));
    }

    @Override
    public void setBottom(int slot) {
      bottom = order[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return (int)(factor * (docOrder.get(bottom) - docOrder.get(doc)));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      order[slot] = doc; // Or is this (int)docOrder.get(doc);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase)
                                                            throws IOException {
      // Ignore as we only support a single reader in this proof of concept
    }

    @Override
    public Comparable<?> value(int slot) {
      try {
        return reader.getTermText(
            (int)termOrder.get((int)docOrder.get(order[slot])));
      } catch (IOException e) {
        throw new RuntimeException(
            "IOException while extracting term String", e);
      }
    }
  }
}
