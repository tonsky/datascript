package datascript;

import java.util.*;

public class Stitch<T> {
  
  public static <T> T[] copy(T[] src, int from, int to, T[] target, int offset) {
    System.arraycopy(src, from, target, offset, to-from);
    return target;
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