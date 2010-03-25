package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;

/**
 * Provides ordinal->String lookup and feeds it to a Comparator.
 */
public class ExposedComparatorWrapper implements ExposedReader.IntComparator {
  private final ExposedReader reader;
  private final Comparator<Object> comparator;

  // Comparator<Object> as Java's Collator is not Comparator<String>
  public ExposedComparatorWrapper(
      ExposedReader reader, Comparator<Object> comparator) {
    this.comparator = comparator;
    this.reader = reader;
  }

  public final int compare(final int value1, final int value2) {
    try {
      return comparator.compare(
          reader.getTermText(value1), reader.getTermText(value2));
    } catch (IOException e) {
      throw new RuntimeException(
          "IOException encountered while comparing terms for the ordinals "
              + value1 + " and " + value2, e);
    }
  }
}
