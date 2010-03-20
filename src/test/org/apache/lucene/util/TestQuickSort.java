package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class TestQuickSort extends LuceneTestCase {

  public void testMonkeyTest() {
    int[] SIZES = new int[]{1, 10, 1000};
    Random random = new Random(87);
    Comparator<Integer> myComparator = new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
        return o1.compareTo(o2);
      }
    };

    for (int size: SIZES) {
      System.out.println("");
      Integer[] source = new Integer[size];
      for (int i = 0 ; i < source.length ; i++) {
        source[i] = random.nextInt(Integer.MAX_VALUE);
      }

      Integer[] mergeSorted = new Integer[source.length];
      System.arraycopy(source, 0, mergeSorted, 0, source.length);
      Arrays.sort(mergeSorted, myComparator);

      Integer[] quickSorted = new Integer[source.length];
      System.arraycopy(source, 0, quickSorted, 0, source.length);
      QuickSort.sort(quickSorted, myComparator);
      
      for (int i = 0 ; i < mergeSorted.length ; i++) {
        System.out.println(mergeSorted[i] + " " + quickSorted[i]);
        assertEquals("The values at index " + i + " should match",
            mergeSorted[i], quickSorted[i]);
      }
    }
  }
}
