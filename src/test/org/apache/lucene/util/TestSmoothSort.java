package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class TestSmoothSort extends LuceneTestCase {

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

      Integer[] smoothSorted = new Integer[source.length];
      System.arraycopy(source, 0, smoothSorted, 0, source.length);
      SmoothSort.sort(smoothSorted, myComparator);

      for (int i = 0 ; i < mergeSorted.length ; i++) {
//        System.out.println(mergeSorted[i] + " " + smoothSorted[i]);
        assertEquals("The values at index " + i + " should match",
            mergeSorted[i], smoothSorted[i]);
      }
    }
  }
}