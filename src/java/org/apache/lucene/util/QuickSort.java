package org.apache.lucene.util;

import java.util.Comparator;

/**
 * Created to experiment with different sorters in ExposedSegmentReader
 */
// http://www.java-tips.org/java-se-tips/java.lang/quick-sort-implementation-with-median-of-three-partitioning-and-cutoff-for-small-a.html
  // WARNING: Not working
public class QuickSort {

  /**
      * Quicksort algorithm.
      * @param a an array of Comparable items.
      */
     public static <T> void sort( T [ ] a, Comparator<? super T> c ) {
         sort( a, 0, a.length - 1, c );
     }

     private static final int CUTOFF = 10;

     /**
      * Internal quicksort method that makes recursive calls.
      * Uses median-of-three partitioning and a cutoff of 10.
      * @param a an array of Comparable items.
      * @param low the left-most index of the subarray.
      * @param high the right-most index of the subarray.
      */
     public static <T> void sort(T[ ] a, int low, int high, Comparator<? super T> c) {
         if( low + CUTOFF > high )
             insertionSort( a, low, high, c );
         else {
             // Sort low, middle, high
             int middle = ( low + high ) / 2;
             if( c.compare(a[ middle ],  a[ low ] ) < 0 )
                 swapReferences( a, low, middle );
             if( c.compare(a[ high ], a[ low ] ) < 0 )
                 swapReferences( a, low, high );
             if( c.compare(a[ high ],  a[ middle ] ) < 0 )
                 swapReferences( a, middle, high );

             // Place pivot at position high - 1
             swapReferences( a, middle, high - 1 );
             T pivot = a[ high - 1 ];

             // Begin partitioning
             int i, j;
             for( i = low, j = high - 1; ; ) {
                 while( c.compare(a[ ++i ], pivot ) < 0 )
                     ;
                 while( c.compare(pivot,  a[ --j ] ) < 0 )
                     ;
                 if( i >= j )
                     break;
                 swapReferences( a, i, j );
             }

             // Restore pivot
             swapReferences( a, i, high - 1 );

             sort( a, low, i - 1, c );    // Sort small elements
             sort( a, i + 1, high, c );   // Sort large elements
         }
     }

     /**
      * Method to swap to elements in an array.
      * @param a an array of objects.
      * @param index1 the index of the first object.
      * @param index2 the index of the second object.
      */
     public static final void swapReferences( Object [ ] a, int index1, int index2 ) {
         Object tmp = a[ index1 ];
         a[ index1 ] = a[ index2 ];
         a[ index2 ] = tmp;
     }


     /**
      * Internal insertion sort routine for subarrays
      * that is used by quicksort.
      * @param a an array of Comparable items.
      * @param low the left-most index of the subarray.
      */
     private static <T> void insertionSort(T[ ] a, int low, int high, Comparator<? super T> c ) {
         for( int p = low + 1; p <= high; p++ ) {
             T tmp = a[ p ];
             int j;

             for( j = p; j > low && c.compare(tmp, a[ j - 1 ] ) < 0; j-- )
                 a[ j ] = a[ j - 1 ];
             a[ j ] = tmp;
         }
     }
}