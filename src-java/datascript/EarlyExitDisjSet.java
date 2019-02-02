package datascript;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
* DisjoinSet + early exit in transient
*/

@SuppressWarnings("unchecked")
public class EarlyExitDisjSet implements ISortedSet {
  static Leaf[] noLeaves = new Leaf[0];
  static int minLen = 32, maxLen = 64, extraLen = 8;

  private static <T> T[] copy(T[] src, int from, int to, T[] target, int offset) {
    System.arraycopy(src, from, target, offset, to-from);
    return target;
  }

  private static class Stitch<T> {
    T[] target;
    int offset;

    Stitch(T[] target, int offset) {
      this.target = target;
      this.offset = offset;
    }

    Stitch copy(T[] source, int from, int to) {
      if (to >= from) {
        System.arraycopy(source, from, target, offset, to - from);
        offset += to - from;
      }
      return this;
    }

    Stitch add(T val) {
      target[offset] = val;
      ++offset;
      return this;
    }
  }

  static String join(String sep, Object[] arr, int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; ++i)
      sb.append(arr[i].toString() + sep);
    return sb.toString();
  }

  Leaf root;
  int size, depth;
  AtomicBoolean setEdit;

  public static void setMaxLen(int maxLen) {
    EarlyExitDisjSet.maxLen = maxLen;
    EarlyExitDisjSet.minLen = maxLen >>> 1;
  }

  public EarlyExitDisjSet() { this(Comparator.naturalOrder()); }
  public EarlyExitDisjSet(int maxLen) {
    this();
    setMaxLen(maxLen);
  }

  public EarlyExitDisjSet(Comparator cmp) {
    setEdit = new AtomicBoolean(false);
    root = new Leaf(new Object[]{}, 0, cmp, setEdit);
    size = 0;
    depth = 1;
  }

  EarlyExitDisjSet(Leaf root, int size, int depth, AtomicBoolean edit) {
    this.root = root;
    this.size = size;
    this.depth = depth;
    this.setEdit = edit;
  }

  public EarlyExitDisjSet asTransient() {
    return new EarlyExitDisjSet(root, size, depth, new AtomicBoolean(true));
  }

  public EarlyExitDisjSet persistent() {
    setEdit.setPlain(false);
    return this;
  }

  public EarlyExitDisjSet with(Object key) {
    Leaf nodes[] = root.add(key, setEdit);

    if (null == nodes)
      return this;

    if (setEdit.getPlain()) {
      if (1 == nodes.length)
        root = nodes[0];
      if (2 == nodes.length) {
        Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
        root = new Node(keys, nodes, 2, root.cmp, setEdit);
        depth++;
      }
      size++;
      return this;
    }

    if (1 == nodes.length)
      return new EarlyExitDisjSet(nodes[0], size+1, depth, setEdit);
    
    Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
    Leaf newRoot = new Node(keys, nodes, 2, root.cmp, setEdit);
    return new EarlyExitDisjSet(newRoot, size+1, depth+1, setEdit);
  }

  public EarlyExitDisjSet without(Object key) {
    Leaf nodes[] = root.remove(key, null, null, setEdit);

    if (null == nodes) // not in set
      return this;

    if (nodes.length == 0) { // in place update
      assert setEdit.getPlain();
      size--;
      return this;
    }

    Leaf newRoot = nodes[1];
    if (setEdit.getPlain()) {
      if (newRoot instanceof Node && newRoot.len == 1) {
        newRoot = ((Node) newRoot).children[0];
        depth--;
      }
      root = newRoot;
      size--;
      return this;
    }

    if (newRoot instanceof Node && newRoot.len == 1) {
      newRoot = ((Node) newRoot).children[0];
      return new EarlyExitDisjSet(newRoot, size-1, depth-1, setEdit);
    }
    
    return new EarlyExitDisjSet(newRoot, size-1, depth, setEdit);
  }

  public int size() {
    return size;
  }

  public int depth() {
    return depth;
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
    if (sb.charAt(sb.length()-1) == " ".charAt(0))
      sb.delete(sb.length()-1, sb.length());
    sb.append("}");
    return sb.toString();
  }

  public String str() {
    return root.str();
  }


  // ===== LEAF =====

  class Leaf {
    Object[] keys;
    int len;
    Comparator cmp;
    AtomicBoolean leafEdit;
    
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
        return new Leaf(new Object[Math.min(EarlyExitDisjSet.maxLen, len + EarlyExitDisjSet.extraLen)], len, cmp, edit);
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

    boolean editable() {
      return leafEdit.getPlain();
    }

    Leaf[] add(Object key, AtomicBoolean edit) {
      int idx = search(keys, 0, len, key);
      if (idx >= 0) // already in set
        return null;
      
      int ins = -idx - 1;

      // modifying array in place
      if (editable() && len < keys.length) {
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
      if (len < EarlyExitDisjSet.maxLen) {
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

    Leaf[] remove(Object key, Leaf left, Leaf right, AtomicBoolean edit) {
      int idx = search(keys, 0, len, key);
      if (idx < 0) // not in set
        return null;

      int newLen = len-1;

      // nothing to merge
      if (newLen >= EarlyExitDisjSet.minLen || (left == null && right == null)) {

        // transient, can edit in place
        if (editable()) {
          copy(keys, idx+1, len, keys, idx);
          len = newLen;
          if (idx == newLen) // removed last, need to signal new maxKey
            return new Leaf[]{left, this, right};
          return noLeaves;        
        }

        // persistent
        Leaf center = newLeaf(newLen, edit);
        new Stitch(center.keys, 0) 
          .copy(keys, 0, idx)
          .copy(keys, idx+1, len);
        return new Leaf[]{left, center, right};
      }

      // can join with left
      if (left != null && left.len + newLen <= EarlyExitDisjSet.maxLen) {
        Leaf join = newLeaf(left.len + newLen, edit);
        new Stitch(join.keys, 0)
          .copy(left.keys, 0,     left.len)
          .copy(keys,      0,     idx)
          .copy(keys,      idx+1, len);
        return new Leaf[]{ join, right };
      }
      
      // can join with right
      if (right != null && newLen + right.len <= EarlyExitDisjSet.maxLen) {
        Leaf join = newLeaf(newLen + right.len, edit);
        new Stitch(join.keys, 0)
          .copy(keys,       0,     idx)
          .copy(keys,       idx+1, len)
          .copy(right.keys, 0,     right.len);
        return new Leaf[]{ left, join };
      }

      // borrow from left
      if (left != null && (left.editable() || right == null || left.len >= right.len)) {
        int totalLen     = left.len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen,
            leftTail     = left.len - newLeftLen;

        Leaf newLeft, newCenter;

        // prepend to center
        if (editable() && newCenterLen <= keys.length) {
          newCenter = this;
          copy(keys,      idx+1,      len,      keys, leftTail + idx);
          copy(keys,      0,          idx,      keys, leftTail);
          copy(left.keys, newLeftLen, left.len, keys, 0);
          len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, edit);
          new Stitch(newCenter.keys, 0)
            .copy(left.keys, newLeftLen, left.len)
            .copy(keys,      0,          idx)
            .copy(keys,      idx+1,      len);
        }

        // shrink left
        if (left.editable()) {
          newLeft  = left;
          left.len = newLeftLen;
        } else {
          newLeft = newLeaf(newLeftLen, edit);
          copy(left.keys, 0, newLeftLen, newLeft.keys, 0);
        }

        return new Leaf[]{ newLeft, newCenter, right };
      }

      // borrow from right
      if (right != null) {
        int totalLen     = newLen + right.len,
            newCenterLen = totalLen >>> 1,
            newRightLen  = totalLen - newCenterLen,
            rightHead    = right.len - newRightLen;
        
        Leaf newCenter, newRight;
        
        // append to center
        if (editable() && newCenterLen <= keys.length) {
          newCenter = this;
          new Stitch(keys, idx)
            .copy(keys,       idx+1, len)
            .copy(right.keys, 0,     rightHead);
          len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, edit);
          new Stitch(newCenter.keys, 0)
            .copy(keys,       0,     idx)
            .copy(keys,       idx+1, len)
            .copy(right.keys, 0,     rightHead);
        }

        // cut head from right
        if (right.editable()) {
          newRight = right;
          copy(right.keys, rightHead, right.len, right.keys, 0);
          right.len = newRightLen;
        } else {
          newRight = newLeaf(newRightLen, edit);
          copy(right.keys, rightHead, right.len, newRight.keys, 0);
        }

        return new Leaf[]{ left, newCenter, newRight };
      }
      throw new RuntimeException("Unreachable");
    }

    public String str() {
      return "{" + join(" ", keys, len) + "}";
    }
  }


  // ===== NODE =====

  class Node extends Leaf {
    Leaf[] children;
    
    Node(Object[] keys, Leaf[] children, int len, Comparator cmp, AtomicBoolean edit) {
      super(keys, len, cmp, edit);
      this.children = children;
    }

    Node newNode(int len, AtomicBoolean edit) {
      return new Node(new Object[len], new Leaf[len], len, cmp, edit);
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
        if (editable()) {
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
      if (len < EarlyExitDisjSet.maxLen) {
        Node n = newNode(len+1, edit);
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
      if (ins+1 == half1) ++half1;
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

    Leaf[] remove(Object key, Leaf left, Leaf right, AtomicBoolean edit) {
      return remove(key, (Node) left, (Node) right, edit);
    }

    Leaf[] remove(Object key, Node left, Node right, AtomicBoolean edit) {
      int idx = search(keys, 0, len, key);
      if (idx < 0) idx = -idx-1;

      if (idx >= len) // not in set
        return null;
      
      Leaf leftChild  = idx > 0 ? children[idx-1] : null,
           rightChild = idx < len-1 ? children[idx+1] : null;
      Leaf[] nodes = children[idx].remove(key, leftChild, rightChild, edit);

      if (null == nodes) // child signalling element not in set
        return null;

      if (0 == nodes.length) // child signalling nothing to update
        return noLeaves;

      int newLen = len - 1
                   - (leftChild != null ? 1 : 0)
                   - (rightChild != null ? 1 : 0)
                   + (nodes[0] != null ? 1 : 0)
                   + (nodes.length > 1 && nodes[1] != null ? 1 : 0)
                   + (nodes.length > 2 && nodes[2] != null ? 1 : 0);

      // no rebalance needed
      if (newLen >= EarlyExitDisjSet.minLen || (left == null && right == null)) {
        Leaf lastNode = nodes.length > 2 && nodes[2] != null ? nodes[2]
                      : nodes.length > 1 && nodes[1] != null ? nodes[1]
                      : nodes[0];

        // can update in place
        if (editable() && (idx < len-2 || lastNode.maxKey() == keys[len-1] )) {
          Stitch<Object> ks = new Stitch(keys, Math.max(idx-1, 0));
          for (int i = 0; i < nodes.length; ++i)
            if (nodes[i] != null)
              ks.add(nodes[i].maxKey());
          if (newLen != len)
            ks.copy(keys, idx+2, len);

          Stitch<Leaf> cs = new Stitch(children, Math.max(idx-1, 0));
          for (int i = 0; i < nodes.length; ++i)
            if (nodes[i] != null)
              cs.add(nodes[i]);
          if (newLen != len)
            cs.copy(children, idx+2, len);

          len = newLen;
          return noLeaves;
        }

        Node newCenter = newNode(newLen, edit);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(keys, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            ks.add(nodes[i].maxKey());
        ks.copy(keys, idx+2, len);

        Stitch<Leaf> cs = new Stitch(newCenter.children, 0);
        cs.copy(children, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            cs.add(nodes[i]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { left, newCenter, right };
      }

      // can join with left
      if (left != null && left.len + newLen <= EarlyExitDisjSet.maxLen) {
        Node join = newNode(left.len + newLen, edit);

        Stitch<Object> ks = new Stitch(join.keys, 0);
        ks.copy(left.keys, 0, left.len);
        ks.copy(keys,      0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            ks.add(nodes[i].maxKey());
        ks.copy(keys,     idx+2, len);

        Stitch<Leaf> cs = new Stitch(join.children, 0);
        cs.copy(left.children, 0, left.len);
        cs.copy(children,      0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            cs.add(nodes[i]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { join, right };
      }

      // can join with right
      if (right != null && newLen + right.len <= EarlyExitDisjSet.maxLen) {
        Node join = newNode(newLen + right.len, edit);

        Stitch<Object> ks = new Stitch(join.keys, 0);
        ks.copy(keys, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            ks.add(nodes[i].maxKey());
        ks.copy(keys,       idx+2, len);
        ks.copy(right.keys, 0, right.len);

        Stitch<Leaf> cs = new Stitch(join.children, 0);
        cs.copy(children, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            cs.add(nodes[i]);
        cs.copy(children,     idx+2, len);
        cs.copy(right.children, 0, right.len);
        
        return new Leaf[] { left, join };
      }

      // borrow from left
      if (left != null && (right == null || left.len >= right.len)) {
        int totalLen     = left.len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen;

        Node newLeft = newNode(newLeftLen, edit),
             newCenter = newNode(newCenterLen, edit);

        copy(left.keys, 0, newLeftLen, newLeft.keys, 0);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(left.keys, newLeftLen, left.len);
        ks.copy(keys, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            ks.add(nodes[i].maxKey());
        ks.copy(keys, idx+2, len);

        copy(left.children, 0, newLeftLen, newLeft.children, 0);

        Stitch<Leaf> cs = new Stitch(newCenter.children, 0);
        cs.copy(left.children, newLeftLen, left.len);
        cs.copy(children, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            cs.add(nodes[i]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { newLeft, newCenter, right };
      }

      // borrow from right
      if (right != null) {
        int totalLen     = newLen + right.len,
            newCenterLen = totalLen >>> 1,
            newRightLen  = totalLen - newCenterLen,
            rightHead    = right.len - newRightLen;

        Node newCenter = newNode(newCenterLen, edit),
             newRight  = newNode(newRightLen, edit);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(keys, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            ks.add(nodes[i].maxKey());
        ks.copy(keys, idx+2, len);
        ks.copy(right.keys, 0, rightHead);

        copy(right.keys, rightHead, right.len, newRight.keys, 0);

        Stitch<Object> cs = new Stitch(newCenter.children, 0);
        cs.copy(children, 0, idx-1);
        for (int i = 0; i < nodes.length; ++i)
          if (nodes[i] != null)
            cs.add(nodes[i]);
        cs.copy(children, idx+2, len);
        cs.copy(right.children, 0, rightHead);

        copy(right.children, rightHead, right.len, newRight.children, 0);        

        return new Leaf[] { left, newCenter, newRight };
      }

      throw new RuntimeException("Unreachable");
    }

    public String str() {
      StringBuilder sb = new StringBuilder("[");
      for (int i=0; i<len; ++i)
        sb.append(keys[i] + ":" + children[i].str() + " ");
      sb.append("]");
      return sb.toString();
    }
  }


  // ===== ITER =====

  class Iter implements Iterator {
    int maxIdx;
    int[]  indexes; // backwards, leaf at 0 .. root at maxIdx
    Leaf[] leaves;  // same
    boolean isOver;

    Iter(EarlyExitDisjSet set) {
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
}