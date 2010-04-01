package org.apache.lucene.index;

import java.io.IOException;
import java.util.*;

/**
 * Wrapper for ExposedReader that provides a flexible cache.
 * </p><p>
 * For random access, a plain setup with no read-ahead is suitable.
 * </p><p>
 * For straight iteration in ordinal order, a setup with maximum read-ahead
 * is preferable. This is uncommon.
 * </p><p>
 * For iteration in sorted ordinal order, where the ordinal order is fairly
 * aligned to unicode sorting order, a setup with some read-ahead works well.
 * This is a common case.
 * </p><p>
 * For merging chunks (See the SegmentReader.getSortedTermOrdinals), where
 * the ordinal order inside the chunks if fairly aligned to unicode sorting
 * order, a read-ahead works iff {@link #onlyReadAheadIfSpace} is true as this
 * prevents values from the beginning of other chunks from being flushed when
 * values from the current chunk is filled with read-ahead. This requires that
 * the merger removes processed values from the cache explicitely.
 *
 */
public class ExposedCachedReader implements ExposedReader {
  private ExposedReader reader;
  private Map<Integer, String> cache;
  private int cacheSizeNum;
  private final int finalOrdinal; // in the given field (including the stated)

  private int readAheadNum;
  private boolean onlyReadAheadIfSpace = false;
  private boolean stopReadAheadOnExistingValue = true;

  // Stats
  long requests = 0;
  long readAheadRequests = 0;
  long misses = 0;
  long lookupTime = 0;

  public ExposedCachedReader(
      ExposedReader reader, int cacheSize, int finalOrdinal) {
    this(reader, cacheSize, 0, finalOrdinal);
  }

  /**
   *
   * @param reader       the backing reader.
   * @param cacheSize    the maximum number of elements to hold in cache.
   * @param readAhead    the maximum number of lookups that can be performed
   *                     after a plain lookup.
   * @param finalOrdinal the last valid ordinal to lookup. Requests for ordinals
   *                     above this will always return null.
   */
  public ExposedCachedReader(
      ExposedReader reader, int cacheSize, int readAhead, int finalOrdinal) {
    if (readAhead > cacheSize) {
      throw new IllegalArgumentException(String.format(
          "Specifying readAhead %d as larger than cacheSize %d is illegal",
          readAhead, cacheSize));
    }
    this.reader = reader;
    cacheSizeNum = cacheSize;
    cache = new LinkedHashMap<Integer, String>(cacheSize, 1.2f, false) {
      @Override
      protected boolean removeEldestEntry(
              Map.Entry<Integer, String> eldest) {
        return size() > cacheSizeNum;
      }
    };
    this.readAheadNum = readAhead;
    this.finalOrdinal = finalOrdinal;
  }

  public String getTermText(final int ordinal) throws IOException {
    requests++;
    if (ordinal > finalOrdinal) {
      return null;
    }
    String s = cache.get(ordinal);
    if (s == null) {
      misses++;
      int reads = 0;
      long startLookup = System.nanoTime();
      s = reader.getTermText(ordinal + reads);
      lookupTime += System.nanoTime() - startLookup;
      cache.put(ordinal, s);
    }
    readAhead(ordinal+1, readAheadNum);
    return s;
  }

  // Stop at new field
  private void readAhead(int ordinal, int readsLeft) throws IOException {
    while (true) {
      if (readsLeft == 0 || ordinal > finalOrdinal ||
          (onlyReadAheadIfSpace && cache.size() == cacheSizeNum)) {
        break;
      }
      readAheadRequests++;
      String s = cache.get(ordinal);
      if (s == null) {
        misses++;
        s = reader.getTermText(ordinal);
        if (s == null) {
          break;
        }
        cache.put(ordinal, s);
      } else if (stopReadAheadOnExistingValue) {
        break;
      }
      ordinal++;
      readsLeft--;
    }
  }

  public ExposedIterator getExposedTuples(
      String persistenceKey, Comparator<Object> comparator, String field,
      boolean collectDocIDs) throws IOException {
    return reader.getExposedTuples(
        persistenceKey, comparator, field, collectDocIDs);
  }

  /**
   * Removes the cache entry for the given ordinal from cache is present.
   * </p><p>
   * If the user of the cache knows that the String for a given ordinal should
   * not be used again, calling this method helps the cache to perform better.
   * @param ordinal the ordinal for the String to remove.
   * @return the old String if it was present in the cashe.
   */
  public String release(int ordinal) {
    return cache.remove(ordinal);
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    cache.clear();
  }

  /* Mutators below */

  public int getCacheSize() {
    return cacheSizeNum;
  }

  /**
   * Setting the cache size clears the cache.
   * @param cacheSize the new size of the cache, measured in elements.
   */
  public void setCacheSize(int cacheSize) {
    this.cacheSizeNum = cacheSize;
    cache.clear();
  }

  public int getReadAhead() {
    return readAheadNum;
  }

  public void setReadAhead(int readAhead) {
    this.readAheadNum = readAhead;
  }

  public boolean isOnlyReadAheadIfSpace() {
    return onlyReadAheadIfSpace;
  }

  public void setOnlyReadAheadIfSpace(boolean onlyReadAheadIfSpace) {
    this.onlyReadAheadIfSpace = onlyReadAheadIfSpace;
  }

  public boolean isStopReadAheadOnExistingValue() {
    return stopReadAheadOnExistingValue;
  }

  public void setStopReadAheadOnExistingValue(boolean stopReadAheadOnExistingValue) {
    this.stopReadAheadOnExistingValue = stopReadAheadOnExistingValue;
  }
}
