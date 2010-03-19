package org.apache.lucene.index;

import org.apache.lucene.util.packed.DeltaWrapper;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.text.CollationKey;
import java.text.Collator;
import java.util.*;

/**
 * Memory usage: log2(#terms)*terms/8 + #docs*log2(#terms)/8 bytes.
 */
public class ExposedSegmentReader implements ExposedReader {
  private SegmentReader segmentReader;
  private int sortCacheSize = 20000; // 2MB ~ 48 bytes + 2*termLength 
  
  public ExposedSegmentReader(SegmentReader segmentReader) {
    this.segmentReader = segmentReader;
  }

  public synchronized long getTermCount(String field) throws IOException {
    long termCount = 0;
    TermEnum terms = segmentReader.terms(new Term(field, ""));
    if (terms != null) {
      while (terms.term() != null && terms.term().field() != null &&
              terms.term().field.equals(field)) {
        termCount++;
        terms.next();
      }
    }
    return termCount;
  }

  public long getBase(String field) throws IOException {
    TermEnum terms = segmentReader.terms(new Term(field, ""));
    if (terms != null && terms.term() != null && terms.term().field() != null &&
              terms.term().field.equals(field)) {
      return getPosition(terms.term());
    }
    return -1;
  }

  public long getPosition(Term term) throws IOException {
    return segmentReader.core.getTermsReader().getPosition(term);
  }

  public Term getTerm(int position) throws IOException {
    return segmentReader.core.getTermsReader().get(position);
  }

  private long lookupTime = 0;
  private long collatorKeyCreation = 0;
  private long cacheRequests = 0;
  private long cacheMisses = 0;
  PackedInts.Reader getSortedTerms(final Collator collator, String field)
                                                            throws IOException {
    lookupTime = 0;
    cacheRequests = 0;
    cacheMisses = 0;

    long startTime = System.nanoTime();
    int termCount = (int)getTermCount(field);
//    System.out.println("Determined term count to " + termCount + " in "
//            + (System.nanoTime() - startTime) / 1000000.0 + "ms");

    startTime = System.nanoTime();
    Integer[] ordered = new Integer[termCount];
    for (int i = 0 ; i < termCount ; i++) {
      ordered[i] = i;
    }
//    System.out.println("Initialized order array in "
//            + (System.nanoTime() - startTime) / 1000000.0 + "ms");

    startTime = System.nanoTime();
    final int basePos = (int)getBase(field);
//    System.out.println("Determined basePos to " + basePos + " in "
//            + (System.nanoTime() - startTime) / 1000000.0 + "ms");

    startTime = System.nanoTime();
    // Explicit sort aligned to cache
/*    int blockSize = SORT_CACHE_SIZE;
    int from = 0;
    while (from < ordered.length) {
      Arrays.sort(ordered,
              from, Math.min(from + blockSize, ordered.length),
              new CachedStringComparator(collator, SORT_CACHE_SIZE, field));
      from += blockSize;
    }
  */
//    cachedStringMergeSort(ordered, collator, field);
//    cachedCollatorKeyMergeSort(ordered, collator, field);
    cachedStringChunkSort(ordered, collator, field);

    long sortTime = (System.nanoTime() - startTime);
    System.out.println(String.format(
            "Sorted %d Terms in %s out of which %s (%s%%) was lookups and " +
                    "%s (%s%%) was collation key creation. " +
                   "The cache (%d terms) got %d requests with %d (%s%%) misses",
            termCount, nsToString(sortTime),
            nsToString(lookupTime),
            lookupTime * 100 / sortTime, 
            nsToString(collatorKeyCreation),
            collatorKeyCreation * 100 / sortTime,
            sortCacheSize, cacheRequests,
            cacheMisses, cacheMisses * 100 / cacheRequests));

    PackedInts.Mutable packed = PackedInts.getMutable(
            termCount, PackedInts.bitsRequired(termCount));
    for (int i = 0 ; i < termCount ; i++) {
      packed.set(i, ordered[i]);
    }
//    System.out.println("Converted array to PackedInts in "
//            + (System.nanoTime() - startTime) / 1000000.0 + "ms");

    return DeltaWrapper.wrap(packed, basePos);
  }

  private void cachedStringMergeSort(
      Integer[] ordered, Collator collator, String field) throws IOException {
    Arrays.sort(ordered,
            new CachedStringComparator(collator, sortCacheSize, field));
  }

  private void cachedCollatorKeyMergeSort(
      Integer[] ordered, Collator collator, String field) throws IOException {
    Arrays.sort(ordered,
            new CachedKeyComparator(collator, sortCacheSize, field));
  }

  /* Algorithm:
   * Sort the ordered array in chunks of size cache-size.
   * Merge sorted chunks by using a heap to determine which chunk to take the
   * next value from.
   */
  private void cachedStringChunkSort(
      Integer[] ordered, Collator collator, String field) throws IOException {
    long oldCacheMisses = cacheMisses;
    int chunkSize = Math.max(
        getSortCacheSize(), ordered.length / getSortCacheSize());
    int chunkCount = (int) Math.ceil(ordered.length * 1.0 / chunkSize);
    // Consider threading here, but beware memory synchronization issues
    long startTimeMerge = System.nanoTime();
    for (int i = 0 ; i < ordered.length ; i += chunkSize) {
      Arrays.sort(ordered,
          i, Math.min(i + chunkSize, ordered.length),
              new CachedStringComparator(collator, getSortCacheSize(), field));
    }
    System.out.println(String.format(
        "Chunk sorted %d chunks of size %d (cache: %d, total terms: %s)" +
            " sorted in %s with %d cache misses",
        chunkCount, chunkSize, getSortCacheSize(), ordered.length,
        nsToString(System.nanoTime() - startTimeMerge),
        cacheMisses - oldCacheMisses));

    if (chunkSize >= ordered.length) {
      return; // No need for double sort
    }

    // We have sorted chunks. Commence the merging!

    // Merging up to cache-size chunks requires an efficient way to determine
    // the chunk with the lowest value. As locality is not that important with
    // all comparables in cache, we use a heap.
    // The heap contains an index (int) for all active chunks in the term order
    // array. When an index is popped, it is incremented and re-inserted unless
    // it is a block start index in which case it is just discarded.

    long startTimeHeap = System.nanoTime();
    long oldHeapCacheMisses = cacheMisses;
    PriorityQueue<Integer> pq = new PriorityQueue<Integer>(
        chunkCount, new CachedStringComparator(
            collator, getSortCacheSize(), field));
    for (int i = 0 ; i < ordered.length ; i += chunkSize) {
      pq.add(i);
    }
    Integer[] sorted = new Integer[ordered.length];
    for (int i = 0 ; i < sorted.length ; i++) {
      Integer next = pq.poll();
      if (next == null) {
        throw new IllegalStateException(
            "Popping the heap should never return null");
      }
      sorted[i] = next;
      if (++next % chunkSize != 0) {
        pq.add(next);
      }
    }
    System.arraycopy(sorted, 0, ordered, 0, sorted.length);
    System.out.println(String.format(
        "Heap merged %d sorted chunks of size %d (cache: %d, total terms: %s)" +
            " in %s with %d cache misses (%d combined for both sort passes)",
        chunkCount, chunkSize, getSortCacheSize(), ordered.length,
        nsToString(System.nanoTime() - startTimeHeap),
        cacheMisses - oldHeapCacheMisses, cacheMisses - oldCacheMisses));
  }

  private class CachedKeyComparator implements Comparator<Integer> {
    private Collator collator;
    private int cacheSize;
    private int base;

    private CachedKeyComparator(
            Collator collator, int cacheSize, String field) throws IOException {
      this.collator = collator;
      this.cacheSize = cacheSize;
      base = (int)getBase(field);
    }

    Map<Integer, CollationKey> cache =
            new LinkedHashMap<Integer, CollationKey>(cacheSize, 1.2f, true) {
              @Override
              protected boolean removeEldestEntry(
                      Map.Entry<Integer, CollationKey> eldest) {
                return size() > cacheSize;
              }
            };

    private synchronized CollationKey getCollationKey(int index)
                                                          throws IOException {
      cacheRequests++;
      CollationKey key = cache.get(index);
      if (key == null) {
        cacheMisses++;

        long startLookup = System.nanoTime();
        String term = getTerm(base + index).text();
        lookupTime += System.nanoTime() - startLookup;

        long collationStart = System.nanoTime();
        key = collator.getCollationKey(term);
        collatorKeyCreation += System.nanoTime() - collationStart;

        cache.put(index, key);
      }
      return key;
    }

    public int compare(final Integer o1, final Integer o2) {
      try {
        return getCollationKey(o1).compareTo(getCollationKey(o2));
/*          String t1 = getTerm(o1 + basePos).text();
          String t2 = getTerm(o2 + basePos).text();
          return collator.compare(t1, t2);*/
      } catch (IOException e) {
        throw new RuntimeException("Unable to lookup term", e);
      }
    }
  }

  private class CachedStringComparator implements Comparator<Integer> {
    private Collator collator;
    private int cacheSize;
    private int base;

    private CachedStringComparator(
            Collator collator, int cacheSize, String field) throws IOException {
      this.collator = collator;
      this.cacheSize = cacheSize;
      base = (int)getBase(field);
    }

    Map<Integer, String> cache =
            new LinkedHashMap<Integer, String>(cacheSize, 1.2f, true) {
              @Override
              protected boolean removeEldestEntry(
                      Map.Entry<Integer, String> eldest) {
                return size() > cacheSize;
              }
            };

    private synchronized String getString(int index)
                                                          throws IOException {
      cacheRequests++;
      String s = cache.get(index);
      if (s == null) {
        cacheMisses++;

        long startLookup = System.nanoTime();
        s = getTerm(base + index).text();
        lookupTime += System.nanoTime() - startLookup;

        cache.put(index, s);
      }
      return s;
    }

    public int compare(final Integer o1, final Integer o2) {
      try {
        return collator.compare(getString(o1), getString(o2));
      } catch (IOException e) {
        throw new RuntimeException("Unable to lookup term", e);
      }
    }
  }


  static String nsToString(long time) {
    return  time > 10L * 1000 * 1000000 ?
            minutes(time) + " min" :
            time > 2 * 1000000 ?
                    time / 1000000 + "ms" :
                    time + "ns";
  }

  private static String minutes(long num) {
    long min = num / 60 / 1000 / 1000000;
    long left = num - (min * 60 * 1000 * 1000000);
    long sec = left / 1000 / 1000000;
    String s = Long.toString(sec);
    while (s.length() < 2) {
      s = "0" + s;
    }
    return min + ":" + s;
  }

  PackedInts.Reader getSortedDocIDs(
          String field, final PackedInts.Reader termOrder) throws IOException {
    final int basePos = (int)getBase(field);
    // TODO: Handle docs without a term (point to max_value?)
    PackedInts.Mutable sorted = PackedInts.getMutable(
            segmentReader.maxDoc(), PackedInts.bitsRequired(termOrder.size()));
    TermDocs termDocs = segmentReader.termDocs(new Term(field, ""));
    for (int i = 0 ; i < termOrder.size() ; i++) {
      Term term = getTerm((int)termOrder.get(i));
      termDocs.seek(term);
      while (termDocs.next()) {
        sorted.set(termDocs.doc(), i);
      }
    }
    return sorted;
  }

  // TODO: These probably should not be here
  public TermDocs termDocs(Term term) throws IOException {
    return segmentReader.termDocs(term);
  }

  public void close() throws IOException {
    segmentReader.close();
  }

  public int getSortCacheSize() {
    return sortCacheSize;
  }

  public void setSortCacheSize(int sortCacheSize) {
    this.sortCacheSize = sortCacheSize;
  }
}
