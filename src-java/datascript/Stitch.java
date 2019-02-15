package datascript;

import java.util.*;
import clojure.lang.*;

public class Stitch<T> {
  
  public static <T> T[] copy(T[] src, int from, int to, T[] target, int offset) {
    System.arraycopy(src, from, target, offset, to-from);
    return target;
  }

  public static Object indexedToArray(Class type, Indexed coll, int from, int to) {
    int len = to - from;
    Object ret = java.lang.reflect.Array.newInstance(type, len);
    for (int i = 0; i < len; ++i)
      java.lang.reflect.Array.set(ret, i, coll.nth(i+from));
    return ret;
  }

  public static int distinct(Comparator<Object> cmp, Object[] arr) {
    int to = 0;
    for (int idx = 1; idx < arr.length; ++idx) {
      if (cmp.compare(arr[idx], arr[to]) != 0) {
        ++to;
        if (to != idx) arr[to] = arr[idx];
      }
    }
    return to + 1;
  }

  T[] target;
  int offset;

  public Stitch(T[] target, int offset) {
    this.target = target;
    this.offset = offset;
  }

  public Stitch copyAll(T[] source, int from, int to) {
    if (to >= from) {
      System.arraycopy(source, from, target, offset, to - from);
      offset += to - from;
    }
    return this;
  }

  public Stitch copyOne(T val) {
    target[offset] = val;
    ++offset;
    return this;
  }
}