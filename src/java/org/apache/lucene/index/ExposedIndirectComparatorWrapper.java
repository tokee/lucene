package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;

/**
 * Variant of ExposedComparatorWrapper that takes the values as index into an
 * array of ordinals for the terms to retrieve for comparison.
 */
public class ExposedIndirectComparatorWrapper implements ExposedReader.IntComparator {
  private final ExposedReader reader;
  private final int[] map;
  private final Comparator<Object> comparator;

  // Comparator<Object> as Java's Collator is not Comparator<String>
  public ExposedIndirectComparatorWrapper(
      ExposedReader reader, int[] map, Comparator<Object> comparator) {
    this.comparator = comparator;
    this.map = map;
    this.reader = reader;
  }

  public final int compare(final int value1, final int value2) {
    try {
      return comparator.compare(
          reader.getTermText(map[value1]), reader.getTermText(map[value2]));
    } catch (IOException e) {
      throw new RuntimeException(
          "IOException encountered while comparing terms for the ordinals "
              + value1 + " and " + value2, e);
    }
  }
}