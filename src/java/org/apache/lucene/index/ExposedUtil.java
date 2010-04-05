package org.apache.lucene.index;

import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.text.Collator;
import java.util.*;

/**
 * Generic Exposed methods.
 */
public class ExposedUtil {
  /**
   * This method generates two arrays: The first array (called docOrder)
   * contains the order of the sorted documents where
   * {@code docOrder(docID) == termOrder index}.
   * To compare the order of two docIDs, just compare the entries:
   * {@code docOrder[docID1]-docOrder[docID2]}.
   * </p><p>
   * The second array (called termOrder) contains the order of the
   * sorted terms where {@code termOrder(index) == termOrdinal}.
   * To resolve the term for a given document, call
   * {@code getTermString(termOrder.get(docOrder.get(docID)))}.
   * </p><p>
   * docIDs without an associated term will be assigned thr maximum value
   * possible for docOrder. Users can check for this with
   * {@code }docOrder(docID)==PackedInts.maxValue(docOrder.getBitsPerValue)}
   * where the max-value should definitely be cached for speed.
   * @param reader         the reader to extract the information from.
   * @param persistenceKey if not null, sort-structures are stored for later
   *                       reuse. The memory usage is approximately
   *                       {@code #terms*log2(#terms)/8 bytes} but depends on
   *                       implementation.
   *                       It is highly recommended to use persistenceKeys as
   *                       they make reopen faster.
   * @param field          the field to sort on.
   * @param comparator     the comparator for the sorter. Must take Strings as
   *                       arguments.
   * @param useFirstTerm   if true, the first term for any given document is
   *                       used. If false, the last term is used. If the sort
   *                       field contains only a single term/document, it is
   *                       recommended to set this to false as it gives slightly
   *                       better build performance.
   * @return an array of docID -> termOrder index plus
   *         an array of termOrder -> term ordinal.
   * @throws java.io.IOException if the reader could not be accessed.
   */
  public static SortArrays getSortArrays(
      ExposedReader reader, String persistenceKey, String field,
      Comparator<Object> comparator, boolean useFirstTerm) throws IOException {
    // TODO: Determine max termOrdinal and use a PackedInts.Reader instead

    ExposedReader.ExposedIterator tuples =
        reader.getExposedTuples(persistenceKey, comparator, field, true);

    // Sizes are not optimal, but should be safe
    PackedInts.Mutable docOrder = PackedInts.getMutable(
        ((IndexReader)reader).maxDoc(),
        PackedInts.bitsRequired(tuples.getMaxSortedTermsCount()+1));
//    System.out.println("MaxDoc: " + ((IndexReader)reader).maxDoc());
    long max = PackedInts.maxValue(docOrder.getBitsPerValue());
    for (int docID = 0 ; docID < docOrder.size() ; docID++) {
      docOrder.set(docID, max); // Default is max == unassigned
    }
    PackedInts.Mutable termOrder = PackedInts.getMutable(
        (int)tuples.getMaxSortedTermsCount(),
        PackedInts.bitsRequired(tuples.getMaxTermOrdinal()));

    ExposedReader.ExposedTuple last = null;
    int orderIndex = -1;
    while (tuples.hasNext()) {
      ExposedReader.ExposedTuple tuple = tuples.next();
/*      if (!tuple.term.text.equals(reader.getTermText((int)tuple.ordinal))) {
        // TODO: Remove this
        throw new IllegalStateException("The term '" + tuple.term.text
            + " should be resolvable with ordinal " + tuple.ordinal);
      }*/
      if (last == null || !tuple.term.equals(last.term)) {
        // This collapses equal terms from different segments to a single term
        // from the first segment - that's fine, since a term for us is a String
        termOrder.set(++orderIndex, tuple.ordinal);
        last = tuple;
      }
      if (!useFirstTerm || docOrder.get((int)tuple.docID) == max) {
        docOrder.set((int)tuple.docID, orderIndex);
      }

/*      if (!tuple.term.text.equals(reader.getTermText((int)termOrder.get(
          (int)docOrder.get((int)tuple.docID))))) {
        // TODO: Remove this
        throw new IllegalStateException("The term '" + tuple.term.text
            + " should be resolvable with full indirection");
      }
  */
      // Sanity check
/*      if (!(comparator.compare(reader.getTermText((int) termOrder.get((int) docOrder.get((int) tuple.docID))),
      reader.getTermText((int)termOrder.get((int) docOrder.get((int) last.docID)))) <= 0)) {
        System.err.println("Failed!");
      }  */
    }

    // TODO: Consider making an optimize of the two arrays
    return new SortArrays(docOrder, termOrder);
  }

  public static long countUnique(PackedInts.Reader values) {
    List<Long> plainList = new ArrayList<Long>(values.size());
    for (int i = 0 ; i < values.size() ; i++) {
      plainList.add(values.get(i));
    }

    Collections.sort(plainList);
    long uniqueValues = 0;
    long lastValue = -1;
    for (long value: plainList) {
      if (lastValue == -1) {
        lastValue = value;
        uniqueValues++;
      }
      if (lastValue != value) {
        uniqueValues++;
      }
      lastValue = value;
    }
    return uniqueValues;
  }

  public static final class SortArrays {
    public final PackedInts.Reader docOrder;
    public final PackedInts.Reader termOrder;

    public SortArrays(PackedInts.Reader docOrder, PackedInts.Reader termOrder) {
      this.docOrder = docOrder;
      this.termOrder = termOrder;
    }
  }

  /**
   * Collator-wrapper that ensures that null-values are handled by being sorted
   * last.
   * @param comparator the original comparator. Often this is a Collator
   * @return the comparator wrapped so that null-values are sorted last.
   */
  public static Comparator<Object> nullHandler(
      final Comparator<Object> comparator) {
    return new Comparator<Object>() {
      public int compare(Object o1, Object o2) {
        if (o1 == null) {
          return o2 == null ? 0 : 1;
        }
        return o2 == null ? -1 : comparator.compare(o1, o2);
      }
    };
  }

  // TODO: Make getFacetArrays that returns docID -> order* and order -> term
}
