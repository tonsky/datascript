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
* CleanupSet + slice
*/

@SuppressWarnings("unchecked")
public class SliceSet implements ISortedSet {
  static Leaf[] EARLY_EXIT = new Leaf[0],
                UNCHANGED  = new Leaf[0];
  static int minLen = 32, maxLen = 64, extraLen = 8;

  static class Edit {
    volatile boolean value = false;
    Edit(boolean value) { this.value = value; }
    public boolean editable() { return value; }
    public void setEditable(boolean value) { this.value = value; }
  }

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

  static String joinStrings(String sep, Object[] arr, int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; ++i) {
      if (i > 0) sb.append(sep);
      sb.append(arr[i].toString());
    }
    return sb.toString();
  }

  Leaf root;
  int size, depth;
  Edit setEdit;

  public static void setMaxLen(int maxLen) {
    SliceSet.maxLen = maxLen;
    SliceSet.minLen = maxLen >>> 1;
  }

  public SliceSet() { this(Comparator.naturalOrder()); }
  public SliceSet(int maxLen) {
    this();
    setMaxLen(maxLen);
  }

  public SliceSet(Comparator cmp) {
    setEdit = new Edit(false);
    root = new Leaf(new Object[]{}, 0, cmp, setEdit);
    size = 0;
    depth = 1;
  }

  SliceSet(Leaf root, int size, int depth, Edit edit) {
    this.root = root;
    this.size = size;
    this.depth = depth;
    this.setEdit = edit;
  }

  public SliceSet asTransient() {
    return new SliceSet(root, size, depth, new Edit(true));
  }

  public SliceSet persistent() {
    setEdit.setEditable(false);
    return this;
  }

  public SliceSet with(Object key) {
    Leaf nodes[] = root.add(key, setEdit);

    if (UNCHANGED == nodes)
      return this;

    if (setEdit.editable()) {
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
      return new SliceSet(nodes[0], size+1, depth, setEdit);
    
    Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
    Leaf newRoot = new Node(keys, nodes, 2, root.cmp, setEdit);
    return new SliceSet(newRoot, size+1, depth+1, setEdit);
  }

  public SliceSet without(Object key) {
    Leaf nodes[] = root.remove(key, null, null, setEdit);

    if (UNCHANGED == nodes) // not in set
      return this;

    if (nodes == EARLY_EXIT) { // in place update
      size--;
      return this;
    }

    Leaf newRoot = nodes[1];
    if (setEdit.editable()) {
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
      return new SliceSet(newRoot, size-1, depth-1, setEdit);
    }
    
    return new SliceSet(newRoot, size-1, depth, setEdit);
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
    return new Iter(this, null, null, root.cmp);
  }

  public Iterator iterator(Object from) {
    return new Iter(this, from, null, root.cmp);
  }

  public Iterator iterator(Object from, Object to) {
    return new Iter(this, from, to, root.cmp);
  }

  public Iterator iterator(Object from, Object to, Comparator cmp) {
    return new Iter(this, from, to, cmp);
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
    Edit leafEdit;
    
    Leaf(Object[] keys, int len, Comparator cmp, Edit contextEdit) {
      this.keys = keys;
      this.len = len;
      this.cmp = cmp;
      this.leafEdit = contextEdit;
    }

    Object maxKey() {
      return keys[len-1];
    }

    Leaf newLeaf(int len, Edit contextEdit) {
      if (contextEdit.editable())
        return new Leaf(new Object[Math.min(SliceSet.maxLen, len + SliceSet.extraLen)], len, cmp, contextEdit);
      else
        return new Leaf(new Object[len], len, cmp, contextEdit);
    }

    int search(Object[] a, int low, int high, Object key) {
      return Arrays.binarySearch(a, low, high, key, cmp);
    }

    boolean contains(Object o) {
      return search(keys, 0, len, o) >= 0;
    }

    Leaf[] add(Object key, Edit contextEdit) {
      int idx = search(keys, 0, len, key);
      if (idx >= 0) // already in set
        return UNCHANGED;
      
      int ins = -idx-1;

      // modifying array in place
      if (leafEdit.editable() && len < keys.length) {
        if (ins == len) {
          keys[len++] = key;
          return new Leaf[]{this};
        } else {
          copy(keys, ins, len, keys, ins+1);
          keys[ins] = key;
          ++len;
          return EARLY_EXIT;
        }
      }

      // simply adding to array
      if (len < SliceSet.maxLen) {
        Leaf n = newLeaf(len+1, contextEdit);
        new Stitch(n.keys, 0)
          .copy(keys, 0, ins)
          .add(key)
          .copy(keys, ins, len);
        return new Leaf[]{n};
      }

      // splitting
      int half1 = (len+1) >>> 1,
          half2 = len+1-half1;

      // goes to first half
      if (ins < half1) {
        Leaf n1 = newLeaf(half1, contextEdit),
             n2 = newLeaf(half2, contextEdit);
        new Stitch(n1.keys, 0)
          .copy(keys, 0, ins)
          .add(key)
          .copy(keys, ins, half1-1);
        copy(keys, half1-1, len, n2.keys, 0);
        return new Leaf[]{n1, n2};
      }

      // copy first, insert to second
      Leaf n1 = newLeaf(half1, contextEdit),
           n2 = newLeaf(half2, contextEdit);
      copy(keys, 0, half1, n1.keys, 0);
      new Stitch(n2.keys, 0)
        .copy(keys, half1, ins)
        .add(key)
        .copy(keys, ins, len);
      return new Leaf[]{n1, n2};
    }

    Leaf[] remove(Object key, Leaf left, Leaf right, Edit contextEdit) {
      int idx = search(keys, 0, len, key);
      if (idx < 0) // not in set
        return UNCHANGED;

      int newLen = len-1;

      // nothing to merge
      if (newLen >= SliceSet.minLen || (left == null && right == null)) {

        // transient, can edit in place
        if (leafEdit.editable()) {
          copy(keys, idx+1, len, keys, idx);
          len = newLen;
          if (idx == newLen) // removed last, need to signal new maxKey
            return new Leaf[]{left, this, right};
          return EARLY_EXIT;        
        }

        // persistent
        Leaf center = newLeaf(newLen, contextEdit);
        new Stitch(center.keys, 0) 
          .copy(keys, 0, idx)
          .copy(keys, idx+1, len);
        return new Leaf[] { left, center, right };
      }

      // can join with left
      if (left != null && left.len + newLen <= SliceSet.maxLen) {
        Leaf join = newLeaf(left.len + newLen, contextEdit);
        new Stitch(join.keys, 0)
          .copy(left.keys, 0,     left.len)
          .copy(keys,      0,     idx)
          .copy(keys,      idx+1, len);
        return new Leaf[] { null, join, right };
      }
      
      // can join with right
      if (right != null && newLen + right.len <= SliceSet.maxLen) {
        Leaf join = newLeaf(newLen + right.len, contextEdit);
        new Stitch(join.keys, 0)
          .copy(keys,       0,     idx)
          .copy(keys,       idx+1, len)
          .copy(right.keys, 0,     right.len);
        return new Leaf[]{ left, join, null };
      }

      // borrow from left
      if (left != null && (left.leafEdit.editable() || right == null || left.len >= right.len)) {
        int totalLen     = left.len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen,
            leftTail     = left.len - newLeftLen;

        Leaf newLeft, newCenter;

        // prepend to center
        if (leafEdit.editable() && newCenterLen <= keys.length) {
          newCenter = this;
          copy(keys,      idx+1,      len,      keys, leftTail + idx);
          copy(keys,      0,          idx,      keys, leftTail);
          copy(left.keys, newLeftLen, left.len, keys, 0);
          len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, contextEdit);
          new Stitch(newCenter.keys, 0)
            .copy(left.keys, newLeftLen, left.len)
            .copy(keys,      0,          idx)
            .copy(keys,      idx+1,      len);
        }

        // shrink left
        if (left.leafEdit.editable()) {
          newLeft  = left;
          left.len = newLeftLen;
        } else {
          newLeft = newLeaf(newLeftLen, contextEdit);
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
        if (leafEdit.editable() && newCenterLen <= keys.length) {
          newCenter = this;
          new Stitch(keys, idx)
            .copy(keys,       idx+1, len)
            .copy(right.keys, 0,     rightHead);
          len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, contextEdit);
          new Stitch(newCenter.keys, 0)
            .copy(keys,       0,     idx)
            .copy(keys,       idx+1, len)
            .copy(right.keys, 0,     rightHead);
        }

        // cut head from right
        if (right.leafEdit.editable()) {
          newRight = right;
          copy(right.keys, rightHead, right.len, right.keys, 0);
          right.len = newRightLen;
        } else {
          newRight = newLeaf(newRightLen, contextEdit);
          copy(right.keys, rightHead, right.len, newRight.keys, 0);
        }

        return new Leaf[]{ left, newCenter, newRight };
      }
      throw new RuntimeException("Unreachable");
    }

    public String str() {
      return "{" + joinStrings(" ", keys, len) + "}";
    }
  }


  // ===== NODE =====

  class Node extends Leaf {
    Leaf[] children;
    
    Node(Object[] keys, Leaf[] children, int len, Comparator cmp, Edit contextEdit) {
      super(keys, len, cmp, contextEdit);
      this.children = children;
    }

    Node newNode(int len, Edit contextEdit) {
      return new Node(new Object[len], new Leaf[len], len, cmp, contextEdit);
    }

    boolean contains(Object o) {
      int idx = search(keys, 0, len-1, o);
      if (idx >= 0) return true;
      return children[-idx-1].contains(o);
    }

    Leaf[] add(Object key, Edit contextEdit) {
      int idx = search(keys, 0, len-1, key);
      if (idx >= 0) // already in set
        return UNCHANGED;
      
      int ins = -idx - 1;
      Leaf[] nodes = children[ins].add(key, contextEdit);

      if (UNCHANGED == nodes) // child signalling already in set
        return UNCHANGED;

      if (EARLY_EXIT == nodes) // child signalling nothing to update
        return EARLY_EXIT;
      
      // same len
      if (1 == nodes.length) {
        Leaf node = nodes[0];
        if (leafEdit.editable()) {
          keys[ins] = node.maxKey();
          children[ins] = node;
          return ins==len-1 && node.maxKey() == maxKey() ? new Leaf[]{this} : EARLY_EXIT;
        }

        Object[] newKeys;
        if (0 == cmp.compare(node.maxKey(), keys[ins]))
          newKeys = keys;
        else {
          newKeys = Arrays.copyOfRange(keys, 0, len);
          newKeys[ins] = node.maxKey();
        }

        Leaf[] newChildren;
        if (node == children[ins])
          newChildren = children;
        else {
          newChildren = Arrays.copyOfRange(children, 0, len);
          newChildren[ins] = node;
        }

        return new Leaf[]{new Node(newKeys, newChildren, len, cmp, contextEdit)};
      }

      // len + 1
      if (len < SliceSet.maxLen) {
        Node n = newNode(len+1, contextEdit);
        new Stitch(n.keys, 0)
          .copy(keys, 0, ins)
          .add(nodes[0].maxKey())
          .add(nodes[1].maxKey())
          .copy(keys, ins+1, len);

        new Stitch(n.children, 0)
          .copy(children, 0, ins)
          .add(nodes[0])
          .add(nodes[1])
          .copy(children, ins+1, len);
        return new Leaf[]{n};
      }

      // split
      int half1 = (len+1) >> 1;
      if (ins+1 == half1) --half1;
      int half2 = len+1-half1;

      // add to first half
      if (ins < half1) {
        Object keys1[] = new Object[half1];
        new Stitch(keys1, 0)
          .copy(keys, 0, ins)
          .add(nodes[0].maxKey())
          .add(nodes[1].maxKey())
          .copy(keys, ins+1, half1-1);
        Object keys2[] = new Object[half2];
        copy(keys, half1-1, len, keys2, 0);

        Leaf children1[] = new Leaf[half1];
        new Stitch(children1, 0)
          .copy(children, 0, ins)
          .add(nodes[0])
          .add(nodes[1])
          .copy(children, ins+1, half1-1);
        Leaf children2[] = new Leaf[half2];
        copy(children, half1-1, len, children2, 0);
        return new Leaf[]{new Node(keys1, children1, half1, cmp, contextEdit),
                          new Node(keys2, children2, half2, cmp, contextEdit)};
      }

      // add to second half
      Object keys1[] = new Object[half1],
             keys2[] = new Object[half2];
      copy(keys, 0, half1, keys1, 0);

      new Stitch(keys2, 0)
        .copy(keys, half1, ins)
        .add(nodes[0].maxKey())
        .add(nodes[1].maxKey())
        .copy(keys, ins+1, len);

      Leaf children1[] = new Leaf[half1],
           children2[] = new Leaf[half2];
      copy(children, 0, half1, children1, 0);

      new Stitch(children2, 0)
        .copy(children, half1, ins)
        .add(nodes[0])
        .add(nodes[1])
        .copy(children, ins+1, len);
      return new Leaf[]{new Node(keys1, children1, half1, cmp, contextEdit),
                        new Node(keys2, children2, half2, cmp, contextEdit)};
    }

    Leaf[] remove(Object key, Leaf left, Leaf right, Edit contextEdit) {
      return remove(key, (Node) left, (Node) right, contextEdit);
    }

    Leaf[] remove(Object key, Node left, Node right, Edit contextEdit) {
      int idx = search(keys, 0, len, key);
      if (idx < 0) idx = -idx-1;

      if (idx >= len) // not in set
        return UNCHANGED;
      
      Leaf leftChild  = idx > 0 ? children[idx-1] : null,
           rightChild = idx < len-1 ? children[idx+1] : null;
      Leaf[] nodes = children[idx].remove(key, leftChild, rightChild, contextEdit);

      if (UNCHANGED == nodes) // child signalling element not in set
        return UNCHANGED;

      if (EARLY_EXIT == nodes) // child signalling nothing to update
        return EARLY_EXIT;

      int newLen = len - 1
                   - (leftChild != null ? 1 : 0)
                   - (rightChild != null ? 1 : 0)
                   + (nodes[0] != null ? 1 : 0)
                   + (nodes[1] != null ? 1 : 0)
                   + (nodes[2] != null ? 1 : 0);

      // no rebalance needed
      if (newLen >= SliceSet.minLen || (left == null && right == null)) {
        // can update in place
        if (leafEdit.editable() && idx < len-2) {
          Stitch<Object> ks = new Stitch(keys, Math.max(idx-1, 0));
          if (nodes[0] != null) ks.add(nodes[0].maxKey());
          if (nodes[1] != null) ks.add(nodes[1].maxKey());
          if (nodes[2] != null) ks.add(nodes[2].maxKey());
          if (newLen != len)
            ks.copy(keys, idx+2, len);

          Stitch<Leaf> cs = new Stitch(children, Math.max(idx-1, 0));
          if (nodes[0] != null) cs.add(nodes[0]);
          if (nodes[1] != null) cs.add(nodes[1]);
          if (nodes[2] != null) cs.add(nodes[2]);
          if (newLen != len)
            cs.copy(children, idx+2, len);

          len = newLen;
          return EARLY_EXIT;
        }

        Node newCenter = newNode(newLen, contextEdit);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(keys, 0, idx-1);
        if (nodes[0] != null) ks.add(nodes[0].maxKey());
        if (nodes[1] != null) ks.add(nodes[1].maxKey());
        if (nodes[2] != null) ks.add(nodes[2].maxKey());
        ks.copy(keys, idx+2, len);

        Stitch<Leaf> cs = new Stitch(newCenter.children, 0);
        cs.copy(children, 0, idx-1);
        if (nodes[0] != null) cs.add(nodes[0]);
        if (nodes[1] != null) cs.add(nodes[1]);
        if (nodes[2] != null) cs.add(nodes[2]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { left, newCenter, right };
      }

      // can join with left
      if (left != null && left.len + newLen <= SliceSet.maxLen) {
        Node join = newNode(left.len + newLen, contextEdit);

        Stitch<Object> ks = new Stitch(join.keys, 0);
        ks.copy(left.keys, 0, left.len);
        ks.copy(keys,      0, idx-1);
        if (nodes[0] != null) ks.add(nodes[0].maxKey());
        if (nodes[1] != null) ks.add(nodes[1].maxKey());
        if (nodes[2] != null) ks.add(nodes[2].maxKey());
        ks.copy(keys,     idx+2, len);

        Stitch<Leaf> cs = new Stitch(join.children, 0);
        cs.copy(left.children, 0, left.len);
        cs.copy(children,      0, idx-1);
        if (nodes[0] != null) cs.add(nodes[0]);
        if (nodes[1] != null) cs.add(nodes[1]);
        if (nodes[2] != null) cs.add(nodes[2]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { null, join, right };
      }

      // can join with right
      if (right != null && newLen + right.len <= SliceSet.maxLen) {
        Node join = newNode(newLen + right.len, contextEdit);

        Stitch<Object> ks = new Stitch(join.keys, 0);
        ks.copy(keys, 0, idx-1);
        if (nodes[0] != null) ks.add(nodes[0].maxKey());
        if (nodes[1] != null) ks.add(nodes[1].maxKey());
        if (nodes[2] != null) ks.add(nodes[2].maxKey());
        ks.copy(keys,       idx+2, len);
        ks.copy(right.keys, 0, right.len);

        Stitch<Leaf> cs = new Stitch(join.children, 0);
        cs.copy(children, 0, idx-1);
        if (nodes[0] != null) cs.add(nodes[0]);
        if (nodes[1] != null) cs.add(nodes[1]);
        if (nodes[2] != null) cs.add(nodes[2]);
        cs.copy(children,     idx+2, len);
        cs.copy(right.children, 0, right.len);
        
        return new Leaf[] { left, join, null };
      }

      // borrow from left
      if (left != null && (right == null || left.len >= right.len)) {
        int totalLen     = left.len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen;

        Node newLeft   = newNode(newLeftLen,   contextEdit),
             newCenter = newNode(newCenterLen, contextEdit);

        copy(left.keys, 0, newLeftLen, newLeft.keys, 0);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(left.keys, newLeftLen, left.len);
        ks.copy(keys, 0, idx-1);
        if (nodes[0] != null) ks.add(nodes[0].maxKey());
        if (nodes[1] != null) ks.add(nodes[1].maxKey());
        if (nodes[2] != null) ks.add(nodes[2].maxKey());
        ks.copy(keys, idx+2, len);

        copy(left.children, 0, newLeftLen, newLeft.children, 0);

        Stitch<Leaf> cs = new Stitch(newCenter.children, 0);
        cs.copy(left.children, newLeftLen, left.len);
        cs.copy(children, 0, idx-1);
        if (nodes[0] != null) cs.add(nodes[0]);
        if (nodes[1] != null) cs.add(nodes[1]);
        if (nodes[2] != null) cs.add(nodes[2]);
        cs.copy(children, idx+2, len);

        return new Leaf[] { newLeft, newCenter, right };
      }

      // borrow from right
      if (right != null) {
        int totalLen     = newLen + right.len,
            newCenterLen = totalLen >>> 1,
            newRightLen  = totalLen - newCenterLen,
            rightHead    = right.len - newRightLen;

        Node newCenter = newNode(newCenterLen, contextEdit),
             newRight  = newNode(newRightLen,  contextEdit);

        Stitch<Object> ks = new Stitch(newCenter.keys, 0);
        ks.copy(keys, 0, idx-1);
        if (nodes[0] != null) ks.add(nodes[0].maxKey());
        if (nodes[1] != null) ks.add(nodes[1].maxKey());
        if (nodes[2] != null) ks.add(nodes[2].maxKey());
        ks.copy(keys, idx+2, len);
        ks.copy(right.keys, 0, rightHead);

        copy(right.keys, rightHead, right.len, newRight.keys, 0);

        Stitch<Object> cs = new Stitch(newCenter.children, 0);
        cs.copy(children, 0, idx-1);
        if (nodes[0] != null) cs.add(nodes[0]);
        if (nodes[1] != null) cs.add(nodes[1]);
        if (nodes[2] != null) cs.add(nodes[2]);
        cs.copy(children, idx+2, len);
        cs.copy(right.children, 0, rightHead);

        copy(right.children, rightHead, right.len, newRight.children, 0);        

        return new Leaf[] { left, newCenter, newRight };
      }

      throw new RuntimeException("Unreachable");
    }

    public String str() {
      StringBuilder sb = new StringBuilder("[");
      for (int i=0; i<len; ++i) {
        if (i > 0) sb.append(" ");
        sb.append(keys[i] + ":" + children[i].str());
      }
      sb.append("]");
      return sb.toString();
    }
  }


  // ===== ITER =====

  class Iter implements Iterator {
    int maxIdx;
    int[]  indexes; // backwards, leaf at 0 .. root at maxIdx
    Leaf[] nodes;   // same
    Comparator cmp;
    Object keyTo, next;
    boolean isOver = false;

    Iter(SliceSet set, Object keyFrom, Object keyTo, Comparator cmp) {
      maxIdx     = set.depth - 1;
      indexes    = new int[set.depth];
      nodes      = new Leaf[set.depth];
      this.cmp   = cmp;
      this.keyTo = keyTo;

      if (keyFrom != null) seek(set, keyFrom);
      else seek(set);

      update();
    }

    void seek(SliceSet set) {
      nodes[maxIdx] = set.root;
      for (int d = maxIdx-1; d >= 0; --d)
        nodes[d] = ((Node)nodes[d+1]).children[0];
      isOver = set.root.len == 0;
    }

    void seek(SliceSet set, Object keyFrom) {
      nodes[maxIdx] = set.root;
      for (int d = maxIdx; d >= 0; --d) {
        Leaf node = nodes[d];
        int idx = Arrays.binarySearch(node.keys, 0, node.len, keyFrom, cmp);
        if (idx < 0) idx = -idx-1;
        if (idx >= node.len) {
          isOver = true;
          break;
        }
        indexes[d] = idx;
        if (d > 0)
          nodes[d-1] = ((Node) node).children[idx];
      }
    }

    void advance() {
      // fast path
      if (indexes[0]+1 < nodes[0].len) {
        indexes[0]++;
        return;
      }

      // leaf overflow
      for (int d = 1; d <= maxIdx; ++d) {
        if (indexes[d]+1 < nodes[d].len) {
          indexes[d]++;
          Arrays.fill(indexes, 0, d, 0);
          for (int dd = d-1; dd >= 0; --dd)
            nodes[dd] = ((Node)nodes[dd+1]).children[indexes[dd+1]];
          return;
        }
      }

      // end of root
      isOver = true;
    }

    public void update() {
      if (isOver) return;
      next = nodes[0].keys[indexes[0]];
      if (keyTo != null && cmp.compare(next, keyTo) > 0)
        isOver = true;
    }

    public Object next() {
      Object res = next;
      advance();
      update();
      return res;
    }

    public boolean hasNext() {
      return !isOver;
    }
  }
}