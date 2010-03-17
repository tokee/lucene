package org.apache.lucene.index;

import java.io.IOException;

/**
 * Exposes semi-direct access to Terms.
 */
public interface ExposedReader {
  /**
   * Queries the segment for the number of unique Terms in the given Field.
   * As the number does not change during the lifetime of the segment,
   * implementations should cache the value.
   * @param field the field with the Terms that should be counted.
   * @return the number of unique Terms in the field. If the field does not
   *         exist or does not have any Terms, 0 is returned.
   * @throws java.io.IOException if there was a problem accessing the Terms.
   */
  long getTermCount(String field) throws IOException;

  public long getBase(String field) throws IOException;

  // Maps directly to TermInfosReader
  long getPosition(Term term) throws IOException;

  // Maps directly to TermInfosReader
  Term getTerm(int position) throws IOException;

  public void close() throws IOException;
  
}
