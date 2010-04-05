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
  private final boolean sortNullFirst;
  private final boolean useFirstTerm;

  public ExposedFieldComparatorSource(
     IndexReader reader, String persistenceKey, Comparator<Object> comparator,
     boolean sortNullFirst, boolean useFirstTerm) {
    if (!(reader instanceof ExposedReader)) {
      throw new UnsupportedOperationException(
          "The provided IndexReader is not an ExposedReader. " +
              "Currently only DirectoryReader and SegmentReader are Exposed");
    }
    this.reader = (ExposedReader)reader;
    persistenceKeyPrefix = persistenceKey;
    this.comparator = comparator;
    this.sortNullFirst = sortNullFirst;
    this.useFirstTerm = useFirstTerm;
  }

  public ExposedFieldComparatorSource(IndexReader reader, Locale locale) {
    this(reader, locale == null ? "null" : locale.toString(),
        Collator.getInstance(locale), true, true);
  }

  @Override
  public FieldComparator newComparator(
      String fieldname, int numHits, int sortPos, boolean reversed)
                                                            throws IOException {
    return new ExposedFieldComparator(
        reader, fieldname, numHits, sortPos, reversed, useFirstTerm);
  }

  private class ExposedFieldComparator extends FieldComparator {
    private final PackedInts.Reader docOrder;
    private final long undefinedTerm;
    private final PackedInts.Reader termOrder;

    private String fieldname;
    private int numHits;
    private int sortPos;
    private boolean reversed;
    private int factor = 1;
    private int docBase = 0; // Added to all incoming docIDs

    private int[] order; // docID TODO: Consider using termOrder index instead
    private int bottom;  // docID

    public ExposedFieldComparator(
        ExposedReader reader, String fieldname, int numHits, int sortPos,
        boolean reversed, boolean useFirstTerm) throws IOException {
      final String key = persistenceKeyPrefix + "_" + fieldname;
      ExposedUtil.SortArrays sortArrays = cache.get(key);
      if (sortArrays == null) {
        long startTime = System.nanoTime();
        sortArrays = ExposedUtil.getSortArrays(
            reader, key, fieldname, comparator, useFirstTerm);
        System.out.println(
            "Calculated exposed sort structures for field " + fieldname 
                + " in a total of "
                + (System.nanoTime() - startTime) / 1000000 + "ms");
        cache.put(key, sortArrays);
      }
      docOrder = sortArrays.docOrder;
      undefinedTerm =
          PackedInts.maxValue(sortArrays.docOrder.getBitsPerValue());
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
      if (sortNullFirst) {
        long slot1order = docOrder.get(order[slot1]);
        long slot2order = docOrder.get(order[slot2]);
        if (slot1order == undefinedTerm) {
          return slot2order == undefinedTerm ? 0 : -1;
        } else if (slot2order == undefinedTerm) {
          return 1;
        }
        return (int)(factor * (slot1order - slot2order));
      }
      // No check for null as null-values are always assigned the highest index
      return (int)(factor *
          (docOrder.get(order[slot1]) - docOrder.get(order[slot2])));
    }

    @Override
    public void setBottom(int slot) {
      bottom = order[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      if (sortNullFirst) {
        long bottomOrder = docOrder.get(bottom);
        long docOrderR = docOrder.get(doc+docBase);
        if (bottomOrder == undefinedTerm) {
          return docOrderR == undefinedTerm ? 0 : -1;
        } else if (docOrderR == undefinedTerm) {
          return 1;
        }
        return (int)(factor * (bottomOrder - docOrderR));
      }
      // No check for null as null-values are always assigned the highest index
      return (int)(factor * (docOrder.get(bottom) - docOrder.get(doc+docBase)));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      // TODO: Remove this
//      System.out.println("Copy called: order[" + slot + "] = "
//          + doc + "+" + docBase + " = " + (doc + docBase));
      order[slot] = doc+docBase;
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase)
                                                            throws IOException {
      this.docBase = docBase;
    }

    @Override
    public Comparable<?> value(int slot) {
      try { // A bit cryptic as we need to handle the case of no sort term
        final long resolvedDocOrder = docOrder.get(order[slot]);
        // TODO: Remove this
/*        System.out.println("Resolving docID " + slot + " with docOrder entry "
            + resolvedDocOrder + " to term "
            + (resolvedDocOrder == undefinedTerm ? "null" :reader.getTermText(
            (int)termOrder.get((int)resolvedDocOrder))));
  */
        return resolvedDocOrder == undefinedTerm ? null : reader.getTermText(
            (int)termOrder.get((int)resolvedDocOrder));
      } catch (IOException e) {
        throw new RuntimeException(
            "IOException while extracting term String", e);
      }
    }
  }
}
