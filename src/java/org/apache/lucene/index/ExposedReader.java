package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Exposes semi-direct access to Terms.
 */
public interface ExposedReader {
  /**
   * Queries the segment for the number of unique Terms in the given Field.
   * </p><p>
   * Note that uniqueness is guaranteed, also for parallel of multi readers.
   * </p><p>
   * Note that the execution time for this might be non-trivial due to the
   * need for comparing for uniqueness.
   * @param field the field with the Terms that should be counted.
   * @return the number of Terms in the field. If the field does not
   *         exist or does not have any Terms, 0 is returned.
   * @throws java.io.IOException if there was a problem accessing the Terms.
   */
  //long getTermCount(String field) throws IOException;

  /**
   * Locates the ordinal of the given term. Usually this will be done by
   * a combination of binary search and sequential access (see
   * {@link TermInfosReader}), so this should not be used for high volume
   * requests (1000+/sec?) without checking that the implementation is fast
   * enough.
   * @param term the term to look up.
   * @return the position of the term if possible, else -1.
   * @throws IOException if the underlying index could not be accessed.
   */
 // long getPosition(Term term) throws IOException;
  // TODO: How does this work for multiple segments?
  // TODO: Consider removing this from the interface or extending with docIDs

  /**
   * Locates the term given its position (ordinal) in the terms array and
   * returns term.text. Usually
   * this is done by doing a seek in a file, followed by a bit of sequential
   * access. However, if {@code getTerm(position-1)} has just been called,
   * {@code getTerm(position)} will normally be very fast.
   * </p><p>
   * It follows that randomized access to Terms through this method is not
   * normally fast, although still faster than {@link #getPosition(Term)}.
   * 10,000+ requests/sec seems like a good rule of thumb. YMMW,
   * </p><p>
   * This is the default though and implementations might remove this
   * bottleneck.
   * @param ordinal the position of the term.
   * @return the term at the given position.
   * @throws IOException if the underlying index could not be accessed.
   */
  String getTermText(int ordinal) throws IOException;

  /**
   * Closes the reader. No further actions can taken.
   * @throws IOException if the reader could not be closed.
   */
  //void close() throws IOException;

  /*

  Merge sorted ordinals:
  Simple hash-merge, near-sequential logic, random switches (SSD)
  Requires: Terms with ordinals and docID from each segment in sorted order
  Speed improvement: Read bursts of X, where X = cacheSize / segments
  * Multiple instances of same term (okay)

  Facets:
  Dual-pass getOrdinalTerms with docIDs.
  docID -> orderIndex*

  Merge facets:
  Collapse identical terms while performing dual-pass


  Total requirement: Caches ordered ordinals at segment


   */

  /**
   * Delivers the terms for the given field sorted by the given comparator.
   * If collectDocIDs is true, the iterator will also deliver the document IDs
   * one at a time.
   * </p><p>
   * Note: Implementations will normally optimize the sorting, so it is highly
   * recommendable to call this method instead of using {@link #getTerm(int)}.
   * @param persistenceKey if not null, the implementation
   *        might choose to store the order of the sorted ordinals for later
   *        re-use for calls with the same persistenceKey.
   *        Important: As persistence is not guaranteed (it might be a waste
   *        of space to store the order for some implementations), the
   *        comparator needs to be specified each time this method is called.
   *        It is very highly recommended to use persistenceKeys as the cost of
   *        sorting the ordinals for the terms can be substantial
   *        (Rule of thumb: ~1 min/1 million terms on hardware anno 2010).
   * @param comparator determines the order of the terms delivered by iterator.
   *        The comparator will receive {@link Term#text}.
   *        Important: It is highly recommended to use a comparator that is
   *        roughly in sync with Unicode String ordering, such as a Java
   *        Collator. Specifying a comparator that sorts in reverse will lead
   *        to severely increased processing times for most implementations.
   * @param field the field to get the terms from.
   * @param collectDocIDs if true, docIDs are delivered by the iterator.
   *        If false, -1 is returned.
   * @throws IOException if the index could not be accessed.
   * @return the extended terms sorted by the given comparator.
   */       
  Iterator<OrdinalTerm> getOrdinalTerms(
      String persistenceKey, Comparator<Object> comparator, String field,
      boolean collectDocIDs) throws IOException;

  /**
   * When using {@link #getOrdinalTerms}, ordinals, terms and optional
   * docIDs will be delivered to the collector in the order dictated by the
   * comparator. In the case of multiple docIDs for the same ordinal, the
   * collector will be fed once per docID (order not guaranteed). If there are
   * no docIDs, -1 is used.
   */
  public class OrdinalTerm {
    /**
     * The term itself. Note that this is re-used by the iterator, so users of
     * the iterator must finish processing the current term before calling next.
     */
    public Term term;
    /**
     * The ordinal for the Term.
     */
    public long ordinal;
    /**
     * A docID for the term or -1 if docIDs are not requested or not existing.
     * If docIDs are reuested and if there are multiple docIDs for the term,
     * multiple ExtendedTerms with the same term and the same ordinal will be
     * used.
     */
    public long docID;

    public OrdinalTerm(long ordinal, Term term, long docID) {
      this.ordinal = ordinal;
      this.term = term;
      this.docID = docID;
    }
  }

  /**
   * Simple atomic specialization of Comparator<Integer>. Normally used to
   * compare terms by treating the arguments as ordinals and performing lookup
   * in an underlying reader.
   */
  interface IntComparator {
    int compare(int value1, int value2);
  }
}
