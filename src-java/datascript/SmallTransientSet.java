package datascript;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

/**
* EarlyExitSet + increase transients in small chunks
*/

@SuppressWarnings("unchecked")
public class SmallTransientSet implements ISortedSet {
  public static int cnt=0;
  private static Leaf[] leaves0 = new Leaf[0];

  Comparator cmp;
  Leaf root;
  int size;
  AtomicReference<Thread> edit;
  int minLen = 64,
      maxLen = 128,
      extraLen = 8;

  Leaf[] noLeaves() { return leaves0; }

  Leaf[] oneLeaf(Leaf l) {
    return new Leaf[]{l};
  }

  Leaf[] twoLeaves(Leaf l1, Leaf l2) {
    return new Leaf[]{l1, l2};
  }

  public SmallTransientSet() { this(Comparator.naturalOrder()); }
  
  public SmallTransientSet(int maxLen) { 
    this(Comparator.naturalOrder());
    this.maxLen = maxLen;
    this.minLen = maxLen / 2;
  }

  public SmallTransientSet(Comparator cmp) {
    this.cmp = cmp;
    edit = new AtomicReference<Thread>(null);
    root = new Leaf(new Object[]{}, 0, edit);
    size = 0;
  }

  SmallTransientSet(Comparator cmp, Leaf root, int size, AtomicReference<Thread> edit, int minLen, int maxLen) {
    this.cmp = cmp;
    this.root = root;
    this.size = size;
    this.edit = edit;
    this.minLen = minLen;
    this.maxLen = maxLen;
  }

  boolean editable(AtomicReference<Thread> edit) {
    return edit.get() != null;
  }

  int search(Object[] a, int low, int high, Object key) {
    return Arrays.binarySearch(a, low, high, key, cmp);
  }

  class Node extends Leaf {
    Leaf[] children;
    
    Node(Object[] keys, Leaf[] children, int len, AtomicReference<Thread> edit) {
      super(keys, len, edit);
      this.children = children;
    }

    boolean contains(Object o) {
      int idx = search(keys, 0, len-1, o);
      if (idx >= 0) return true;
      return children[-idx-1].contains(o);
    }

    Leaf[] add(Object key, AtomicReference<Thread> edit) {
      int idx = search(keys, 0, len-1, key);
      if (idx >= 0) // already in set
        return null;
      
      int ins = -idx - 1;
      Leaf[] nodes = children[ins].add(key, edit);
      if (null == nodes)
        return null;

      if (0 == nodes.length)
        return noLeaves();
      
      // same len
      if (1 == nodes.length) {
        if (editable(this.edit)) {
          keys[ins] = nodes[0].maxKey();
          children[ins] = nodes[0];
          return ins==len-1 && nodes[0].maxKey() == maxKey() ? oneLeaf(this) : noLeaves();
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

        return oneLeaf(new Node(newKeys, newChildren, len, edit));
      }

      // len + 1
      if (len < maxLen) {
        Node n = new Node(new Object[len+1], new Leaf[len+1], len+1, edit);
        System.arraycopy(keys, 0, n.keys, 0, ins);
        n.keys[ins] = nodes[0].maxKey();
        n.keys[ins+1] = nodes[1].maxKey();
        System.arraycopy(keys, ins+1, n.keys, ins+2, len-ins-1);

        System.arraycopy(children, 0, n.children, 0, ins);
        n.children[ins] = nodes[0];
        n.children[ins+1] = nodes[1];
        System.arraycopy(children, ins+1, n.children, ins+2, len-ins-1);
        return oneLeaf(n);
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
        return twoLeaves(new Node(keys1, children1, half1, edit),
                         new Node(keys2, children2, half2, edit));
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
      return twoLeaves(new Node(keys1, children1, half1, edit),
                       new Node(keys2, children2, half2, edit));
    }

    public String toString() {
      String res = "";
      for (int i=0; i<len; ++i)
        res += children[i].toString();
      return res;
    }
  }

  class Leaf {
    Object[] keys;
    int len;
    AtomicReference<Thread> edit;

    Leaf(Object[] keys, int len, AtomicReference<Thread> edit) {
      this.keys = keys;
      this.len = len;
      this.edit = edit;
    }

    Object maxKey() {
      return keys[len-1];
    }

    Leaf newLeaf(int len, AtomicReference<Thread> edit) {
      if (editable(edit))
        return new Leaf(new Object[Math.min(maxLen, len + extraLen)], len, edit);
      else
        return new Leaf(new Object[len], len, edit);
    }

    boolean contains(Object o) {
      return search(keys, 0, len, o) >= 0;
    }

    Leaf[] add(Object key, AtomicReference<Thread> edit) {
      int idx = search(keys, 0, len, key);
      if (idx >= 0) // already in set
        return null;
      
      int ins = -idx - 1;
 
      // modifying array in place
      if (editable(this.edit) && len < keys.length) {
        if (ins == len) {
          keys[len++] = key;
          return oneLeaf(this);
        } else {
          System.arraycopy(keys, ins, keys, ins+1, len-ins);
          keys[ins] = key;
          ++len;
          return noLeaves();
        }
      }

      // simply adding to array
      if (len < maxLen) {
        Leaf n = newLeaf(len+1, edit);
        System.arraycopy(keys, 0, n.keys, 0, ins);
        n.keys[ins] = key;
        System.arraycopy(keys, ins, n.keys, ins+1, len-ins);
        return oneLeaf(n);
      }

      // splitting
      int half1 = (len+1) >> 1,
          half2 = len+1-half1;

      // goes to first half
      if (ins < half1) {
        Leaf n1 = newLeaf(half1, edit),
             n2 = newLeaf(half2, edit);
        System.arraycopy(keys, 0, n1.keys, 0, ins);
        n1.keys[ins] = key;
        System.arraycopy(keys, ins, n1.keys, ins+1, half1-ins-1);
        System.arraycopy(keys, half1-1, n2.keys, 0, half2);
        return twoLeaves(n1, n2);
      }

      // copy first, insert to second
      Leaf n1 = newLeaf(half1, edit),
           n2 = newLeaf(half2, edit);
      System.arraycopy(keys, 0, n1.keys, 0, half1);
      System.arraycopy(keys, half1, n2.keys, 0, ins-half1);
      n2.keys[ins-half1] = key;
      System.arraycopy(keys, ins, n2.keys, ins-half1+1, len-ins);
      return twoLeaves(n1, n2);
    }

    public String toString() {
      String res = "";
      for (int i=0; i<len; ++i)
        res += keys[i].toString() + ", ";
      return res;
    }
  }

  public SmallTransientSet asTransient() {
    return new SmallTransientSet(cmp, root, size, new AtomicReference<Thread>(Thread.currentThread()), minLen, maxLen);
  }

  public SmallTransientSet persistent() {
    edit.set(null);
    return this;
  }

  public SmallTransientSet with(Object key) {
    Leaf nodes[] = root.add(key, edit);

    if (null == nodes)
      return this;

    if (editable(edit)) {
      if (1 == nodes.length)
        root = nodes[0];
      if (2 == nodes.length)
        root = new Node(new Object[]{nodes[0].maxKey(), nodes[1].maxKey()}, nodes, 2, edit);
      size++;
      return this;
    }

    if (0 == nodes.length)
      return new SmallTransientSet(cmp, root, size+1, edit, minLen, maxLen);

    if (1 == nodes.length)
      return new SmallTransientSet(cmp, nodes[0], size+1, edit, minLen, maxLen);
    
    Object keys[] = new Object[]{nodes[0].maxKey(), nodes[1].maxKey()};
    Leaf newRoot = new Node(keys, nodes, 2, edit);
    return new SmallTransientSet(cmp, newRoot, size+1, edit, minLen, maxLen);
  }

  public int size() {
    return size;
  }

  public boolean contains(Object o) {
    return root.contains(o);
  }

  public String toString() {
    return "#{" + root.toString() + "}";
  }
}