package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntPredicate;

public class Utils {
  // Implementing Fisher–Yates shuffle
  public static void shuffleArray(int[] ar) {
    Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--) {
      int index = rnd.nextInt(i + 1);
      // Simple swap
      int a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }

  public static void smartChoiceIntersect(IntArrayList arr1, IntArrayList arr2,
                                          int i1, int size1, int i2, int size2, IntCollection target) {

//     if (arr1.size() * Math.log(arr2.size()) < arr1.size() + arr2.size()) {
//        Utils.binaryIntersect(arr1, arr2, i1, size1, i2, size2, target);
//     } else if (arr2.size() * Math.log(arr1.size()) < arr1.size() + arr2.size()) {
//        Utils.binaryIntersect(arr2, arr1, i2, size2, i1, size1, target);
//     } else {
        Utils.sintersect(arr1, arr2, i1, size1, i2, size2, target);
//     }
  }

   private static void binaryIntersect(IntArrayList arr1, IntArrayList arr2,
                                       int p1, int size1, int p2, int size2, IntCollection result) {

      while (p1 < size1 && p2 < size2) {
         int v1 = arr1.getUnchecked(p1);
         int v2 = arr2.getUnchecked(p2);
         if (v1 == v2) {
            result.add(v1);
            p1++;
            p2++;
         } else if (v1 < v2) {
            p1=Utils.BinarySearch(arr1,p1+1,v2);
         } else {
            p2 = Utils.BinarySearch(arr2,p2+1, v1);
         }
      }
   }

   private static int BinarySearch(IntArrayList arr, int p, int v) {
      int start = p;
      int end = arr.size() - 1;
      if (start > end) return start;
      while (true) {
         int mid = (start + end) / 2;
         int val = arr.getUnchecked(mid);
         if (mid == start) {
            return val < v ? mid + 1 : mid;
         }
         if (val == v) {
            return mid;
         } else if (val < v) {
            start = mid + 1;
         } else {
            end = mid - 1;
         }
      }
   }


  public static int sintersect(IntArrayList arr1, IntArrayList arr2,
        int i1, int size1, int i2, int size2, IntCollection target) {
     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           target.add(v1);
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           ++i1;
        } else {
           ++i2;
        }
        ++cost;
     }

     return cost;
  }
  
  public static int sintersect(IntArrayList arr1, IntArrayList arr2,
        int i1, int size1, int i2, int size2, IntCollection target,
        IntPredicate pred) {

     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           if (pred.test(v1)) {
              target.add(v1);
           }
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           ++i1;
        } else {
           ++i2;
        }
        ++cost;
     }

     return cost;
  }

  public static int sdifference(IntArrayList arr1, IntArrayList arr2,
        int i1, int size1, int i2, int size2, IntCollection target) {
     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           target.add(v1);
           ++i1;
        } else {
           ++i2;
        }
        ++cost;
     }

     while (i1 < size1) {
        target.add(arr1.getUnchecked(i1));
        ++i1;
     }

     return cost;
  }

  public static int sunion(IntArrayList arr1, IntArrayList arr2,
        int i1, int size1, int i2, int size2, IntCollection target) {
     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           target.add(v1);
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           target.add(v1);
           ++i1;
        } else {
           target.add(v2);
           ++i2;
        }
        ++cost;
     }
     
     while (i1 < size1) {
        target.add(arr1.getUnchecked(i1));
        ++i1;
     }
     
     while (i2 < size2) {
        target.add(arr2.getUnchecked(i2));
        ++i2;
     }

     return cost;
  }
}
