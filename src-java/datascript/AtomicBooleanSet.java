package datascript;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

/**
* ReverseFlatIterSet + extracted inner classes + AtomicBoolean
*/

public class AtomicBooleanSet implements IPersistentSet {
  static int minLen = 32, maxLen = 64, extraLen = 8;

  Leaf root;
  int size, depth;
  AtomicBoolean setEdit;

  static void setMaxLen(int maxLen) {
    AtomicBooleanSet.maxLen = maxLen;
    AtomicBooleanSet.minLen = maxLen >>> 1;
  }

  public AtomicBooleanSet() { this(Comparator.naturalOrder()); }
  public AtomicBooleanSet(int maxLen) {
    this();
    setMaxLen(maxLen);
  }

  public AtomicBooleanSet(Comparator cmp) {
    setEdit = new AtomicBoolean(false);
    root = new Leaf(new Object[]{}, 0, cmp, setEdit);
    size = 0;
    depth = 1;
  }

  AtomicBooleanSet(Leaf root, int size, int depth, AtomicBoolean edit) {
    this.root = root;
    this.size = size;
    this.depth = depth;
    this.setEdit = edit;
  }

  public AtomicBooleanSet asTransient() {
    return new AtomicBooleanSet(root, size, depth, new AtomicBoolean(true));
  }

  public AtomicBooleanSet persistent() {
    setEdit.setPlain(false);
    return this;
  }

  public AtomicBooleanSet add(Object key) {
    Leaf nodes[] = root.add(key, setEdit);

    if (null == nodes)
      return this;

    if (setEdit.getPlain()) {
      if (1 == nodes.length)
        root = nodes[0];
      if (2 == nodes.length) {
        Object keys[] = new Object[]{nodes[0].maxKey(), nodes[1].maxKey()};
        root = new Node(keys, nodes, 2, root.cmp, setEdit);
        depth++;
      }
      size++;
      return this;
    }

    if (0 == nodes.length)
      return new AtomicBooleanSet(root, size+1, depth, setEdit);

    if (1 == nodes.length)
      return new AtomicBooleanSet(nodes[0], size+1, depth, setEdit);
    
    Object keys[] = new Object[]{nodes[0].maxKey(), nodes[1].maxKey()};
    Leaf newRoot = new Node(keys, nodes, 2, root.cmp, setEdit);
    return new AtomicBooleanSet(newRoot, size+1, depth+1, setEdit);
  }

  public int size() {
    return size;
  }

  public boolean contains(Object o) {
    return root.contains(o);
  }

  public Iterator iterator() {
    return new Iter(this);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("#{");
    for(Object o: this)
      sb.append(o).append(" ");
    sb.delete(sb.length()-1, sb.length());
    sb.append("}");
    return sb.toString();
  }
}


class Leaf {
  Object[] keys;
  int len;
  Comparator cmp;
  AtomicBoolean leafEdit;
  
  static Leaf[] noLeaves = new Leaf[0];

  Leaf(Object[] keys, int len, Comparator cmp, AtomicBoolean edit) {
    this.keys = keys;
    this.len = len;
    this.cmp = cmp;
    this.leafEdit = edit;
  }

  Object maxKey() {
    return keys[len-1];
  }

  Leaf newLeaf(int len, AtomicBoolean edit) {
    if (edit.getPlain())
      return new Leaf(new Object[Math.min(AtomicBooleanSet.maxLen, len + AtomicBooleanSet.extraLen)], len, cmp, edit);
    else
      return new Leaf(new Object[len], len, cmp, edit);
  }

  int search(Object[] a, int low, int high, Object key) {
    // if (high-low > 32)
    //   return Arrays.binarySearch(a, low, high, key, cmp);

    for(int i = low; i < high; ++i) {
      final int d = cmp.compare(a[i], key);
      if (d == 0)
        return i;
      else if (d > 0)
        return -i-1;
    }
    return -high-1;
  }

  boolean contains(Object o) {
    return search(keys, 0, len, o) >= 0;
  }

  Leaf[] add(Object key, AtomicBoolean edit) {
    int idx = search(keys, 0, len, key);
    if (idx >= 0) // already in set
      return null;
    
    int ins = -idx - 1;

    // modifying array in place
    if (leafEdit.getPlain() && len < keys.length) {
      if (ins == len) {
        keys[len++] = key;
        return new Leaf[]{this};
      } else {
        System.arraycopy(keys, ins, keys, ins+1, len-ins);
        keys[ins] = key;
        ++len;
        return noLeaves;
      }
    }

    // simply adding to array
    if (len < AtomicBooleanSet.maxLen) {
      Leaf n = newLeaf(len+1, edit);
      System.arraycopy(keys, 0, n.keys, 0, ins);
      n.keys[ins] = key;
      System.arraycopy(keys, ins, n.keys, ins+1, len-ins);
      return new Leaf[]{n};
    }

    // splitting
    int half1 = (len+1) >>> 1,
        half2 = len+1-half1;

    // goes to first half
    if (ins < half1) {
      Leaf n1 = newLeaf(half1, edit),
           n2 = newLeaf(half2, edit);
      System.arraycopy(keys, 0, n1.keys, 0, ins);
      n1.keys[ins] = key;
      System.arraycopy(keys, ins, n1.keys, ins+1, half1-ins-1);
      System.arraycopy(keys, half1-1, n2.keys, 0, half2);
      return new Leaf[]{n1, n2};
    }

    // copy first, insert to second
    Leaf n1 = newLeaf(half1, edit),
         n2 = newLeaf(half2, edit);
    System.arraycopy(keys, 0, n1.keys, 0, half1);
    System.arraycopy(keys, half1, n2.keys, 0, ins-half1);
    n2.keys[ins-half1] = key;
    System.arraycopy(keys, ins, n2.keys, ins-half1+1, len-ins);
    return new Leaf[]{n1, n2};
  }
}


class Node extends Leaf {
  Leaf[] children;
  
  Node(Object[] keys, Leaf[] children, int len, Comparator cmp, AtomicBoolean edit) {
    super(keys, len, cmp, edit);
    this.children = children;
  }

  boolean contains(Object o) {
    int idx = search(keys, 0, len-1, o);
    if (idx >= 0) return true;
    return children[-idx-1].contains(o);
  }

  Leaf[] add(Object key, AtomicBoolean edit) {
    int idx = search(keys, 0, len-1, key);
    if (idx >= 0) // already in set
      return null;
    
    int ins = -idx - 1;
    Leaf[] nodes = children[ins].add(key, edit);

    if (null == nodes) // child signalling already in set
      return null;

    if (0 == nodes.length) // child signalling nothing to update
      return noLeaves;
    
    // same len
    if (1 == nodes.length) {
      if (leafEdit.getPlain()) {
        keys[ins] = nodes[0].maxKey();
        children[ins] = nodes[0];
        return ins==len-1 && nodes[0].maxKey() == maxKey() ? new Leaf[]{this} : noLeaves;
      }

      Object[] newKeys;
      if (0 == cmp.compare(nodes[0].maxKey(), keys[ins]))
        newKeys = keys;
      else {
        newKeys = Arrays.copyOfRange(keys, 0, len);
        newKeys[ins] = nodes[0].maxKey();
      }

      Leaf[] newChildren;
      if (nodes[0] == children[ins])
        newChildren = children;
      else {
        newChildren = Arrays.copyOfRange(children, 0, len);
        newChildren[ins] = nodes[0];
      }

      return new Leaf[]{new Node(newKeys, newChildren, len, cmp, edit)};
    }

    // len + 1
    if (len < AtomicBooleanSet.maxLen) {
      Node n = new Node(new Object[len+1], new Leaf[len+1], len+1, cmp, edit);
      System.arraycopy(keys, 0, n.keys, 0, ins);
      n.keys[ins] = nodes[0].maxKey();
      n.keys[ins+1] = nodes[1].maxKey();
      System.arraycopy(keys, ins+1, n.keys, ins+2, len-ins-1);

      System.arraycopy(children, 0, n.children, 0, ins);
      n.children[ins] = nodes[0];
      n.children[ins+1] = nodes[1];
      System.arraycopy(children, ins+1, n.children, ins+2, len-ins-1);
      return new Leaf[]{n};
    }

    // split
    int half1 = (len+1) >> 1;
    if (ins+1 == half1) --half1;
    int half2 = len+1-half1;

    // add to first half
    if (ins < half1) {
      Object keys1[] = new Object[half1];
      System.arraycopy(keys, 0, keys1, 0, ins);
      keys1[ins] = nodes[0].maxKey();
      keys1[ins+1] = nodes[1].maxKey();
      System.arraycopy(keys, ins+1, keys1, ins+2, half1-ins-2);
      Object keys2[] = Arrays.copyOfRange(keys, half1-1, len);

      Leaf children1[] = new Leaf[half1];
      System.arraycopy(children, 0, children1, 0, ins);
      children1[ins] = nodes[0];
      children1[ins+1] = nodes[1];
      System.arraycopy(children, ins+1, children1, ins+2, half1-ins-2);
      Leaf children2[] = Arrays.copyOfRange(children, half1-1, len);
      return new Leaf[]{new Node(keys1, children1, half1, cmp, edit),
                        new Node(keys2, children2, half2, cmp, edit)};
    }

    // add to second half
    Object keys1[] = Arrays.copyOfRange(keys, 0, half1);
    Object keys2[] = new Object[half2];
    System.arraycopy(keys, half1, keys2, 0, ins-half1);
    keys2[ins-half1] = nodes[0].maxKey();
    keys2[ins-half1+1] = nodes[1].maxKey();
    System.arraycopy(keys, ins+1, keys2, ins-half1+2, len-ins-1);

    Leaf children1[] = Arrays.copyOfRange(children, 0, half1);
    Leaf children2[] = new Leaf[half2];
    System.arraycopy(children, half1, children2, 0, ins-half1);
    children2[ins-half1] = nodes[0];
    children2[ins-half1+1] = nodes[1];
    System.arraycopy(children, ins+1, children2, ins-half1+2, len-ins-1); 
    return new Leaf[]{new Node(keys1, children1, half1, cmp, edit),
                      new Node(keys2, children2, half2, cmp, edit)};
  }
}


class Iter implements Iterator {
  int maxIdx;
  int[]  indexes; // backwards, leaf at 0 .. root at maxIdx
  Leaf[] leaves;  // same
  boolean isOver;

  Iter(AtomicBooleanSet set) {
    maxIdx  = set.depth-1;
    indexes = new int[set.depth];
    leaves  = new Leaf[set.depth];
    leaves[maxIdx] = set.root;
    for (int d = maxIdx-1; d >= 0; --d)
      leaves[d] = ((Node)leaves[d+1]).children[0];
    isOver = set.root.len == 0;
  }

  Object get() {
    return leaves[0].keys[indexes[0]];
  }

  void advance() {
    // fast path
    if (indexes[0]+1 < leaves[0].len) {
      indexes[0]++;
      return;
    }

    // leaf overflow
    for (int d = 1; d <= maxIdx; ++d) {
      if (indexes[d]+1 < leaves[d].len) {
        indexes[d]++;
        Arrays.fill(indexes, 0, d, 0);
        for (int dd = d-1; dd >= 0; --dd)
          leaves[dd] = ((Node)leaves[dd+1]).children[indexes[dd+1]];
        return;
      }
    }

    // end of root
    isOver = true;
  }

  public Object next() {
    Object res = get();
    advance();
    return res;
  }

  public boolean hasNext() {
    return !isOver;
  }
}
