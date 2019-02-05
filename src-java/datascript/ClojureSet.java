package datascript;

import java.util.*;
import clojure.lang.*;

/**
* CleanupSet + clojure interfaces
*
*   8 max      16 max      32 max      64 max      128 max      256 max      512 max      1024 max
*   
*   100K ADDs
*   23..30ms    19..21ms    19..23ms    19..22ms    19..22ms    20..22ms    21..29ms    24..28ms    
*   
*   100K CONTAINS
*   17..18ms    13..15ms    14..16ms    14..16ms    14..16ms    15..17ms    15..17ms    15..17ms    
*   
*   ITERATE over 1M
*   17..18ms    10..12ms     8..10ms     6.. 8ms     6.. 7ms     5.. 7ms     5.. 6ms     5..18ms    
*   
*   SEQ ITER over 1M
*   21..24ms    17..26ms    16..23ms    14..21ms    14..20ms    14..20ms    13..18ms    13..18ms    
*   
*   100K REMOVEs
*   27..31ms    21..22ms    19..21ms    18..19ms    18..20ms    18..21ms    18..19ms    20..29ms    
*
*/

@SuppressWarnings("unchecked")
public class ClojureSet extends ASortedSet implements IEditableCollection, ITransientCollection {

  static Leaf[] EARLY_EXIT = new Leaf[0],
                UNCHANGED  = new Leaf[0];

  static int MIN_LEN = 32, MAX_LEN = 64, EXTRA_LEN = 8;

  static class Edit {
    public volatile boolean _value = false;
    Edit(boolean value) { _value = value; }
    public boolean editable() { return _value; }
    public void setEditable(boolean value) { _value = value; }
  }

  public static void setMaxLen(int maxLen) {
    MAX_LEN = maxLen;
    MIN_LEN = maxLen >>> 1;
  }

  Leaf _root;
  int _count;
  final Edit _edit;

  public ClojureSet() { this(null, RT.DEFAULT_COMPARATOR); }
  public ClojureSet(Comparator cmp) { this(null, cmp); }
  public ClojureSet(IPersistentMap meta, Comparator cmp) {
    super(meta, cmp);
    _edit  = new Edit(false);
    _root  = new Leaf(new Object[]{}, 0, _edit);
    _count = 0;
  }

  ClojureSet(IPersistentMap meta, Comparator cmp, Leaf root, int count, Edit edit) {
    super(meta, cmp);
    _root  = root;
    _count = count;
    _edit  = edit;
  }

  public Seq slice(Object from, Object to) {
    return slice(from, to, _cmp);
  }

  public Seq slice(Object from, Object to, Comparator cmp) {
    if (_count == 0) return null;
    Seq seq = null;
    Leaf node = _root;
    while (true) {
      int idx = from == null ? 0 : node.search(from, cmp);
      if (idx < 0) idx = -idx-1;
      if (idx == node._len) return null;
      if (node instanceof Node) {
        seq = new Seq(null, seq, node, idx, null, null);
        node = seq.child();
      } else {
        seq = new Seq(null, seq, node, idx, to, cmp);
        break;
      }
    }
    return seq;
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

  public String str() { return _root.str(0); }


  // IObj
  public ClojureSet withMeta(IPersistentMap meta) {
    if(_meta == meta) return this;
    return new ClojureSet(meta, _cmp, _root, _count, _edit);
  }

  // Counted
  public int count() { return _count; }

  //  Seqable
  public Seq seq() {
    return slice(null, null, _cmp);
  }

  // IPersistentCollection
  public ClojureSet empty() {
    return new ClojureSet(_meta, _cmp);
  }

  public ClojureSet cons(Object key) {
    Leaf nodes[] = _root.add(key, _cmp, _edit);

    if (UNCHANGED == nodes)
      return this;

    if (_edit.editable()) {
      if (1 == nodes.length)
        _root = nodes[0];
      if (2 == nodes.length) {
        Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
        _root = new Node(keys, nodes, 2, _edit);
      }
      _count++;
      return this;
    }

    if (1 == nodes.length)
      return new ClojureSet(_meta, _cmp, nodes[0], _count+1, _edit);
    
    Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
    Leaf newRoot = new Node(keys, nodes, 2, _edit);
    return new ClojureSet(_meta, _cmp, newRoot, _count+1, _edit);
  }

  // IPersistentSet
  public ClojureSet disjoin(Object key) { 
    Leaf nodes[] = _root.remove(key, null, null, _cmp, _edit);

    // not in set
    if (UNCHANGED == nodes) return this;
    // in place update
    if (nodes == EARLY_EXIT) { _count--; return this; }
    Leaf newRoot = nodes[1];
    if (_edit.editable()) {
      if (newRoot instanceof Node && newRoot._len == 1)
        newRoot = ((Node) newRoot)._children[0];
      _root = newRoot;
      _count--;
      return this;
    }
    if (newRoot instanceof Node && newRoot._len == 1) {
      newRoot = ((Node) newRoot)._children[0];
      return new ClojureSet(_meta, _cmp, newRoot, _count-1, _edit);
    }
    return new ClojureSet(_meta, _cmp, newRoot, _count-1, _edit);
  }

  public boolean contains(Object key) {
    return _root.contains(key, _cmp);
  }

  // IEditableCollection
  public ClojureSet asTransient() {
    return new ClojureSet(_meta, _cmp, _root, _count, new Edit(true));
  }

  // ITransientCollection
  public ClojureSet conj(Object key) {
    return cons(key);
  }

  public ClojureSet persistent() {
    _edit.setEditable(false);
    return this;
  }

  // Iterable
  public Iterator iterator() {
    return new JavaIter(seq());
  }

  // ISortedSet
  // public ClojureSet(int ml) { this(); setMaxLen(ml); }
  // public ClojureSet with(Object key) { return cons(key); }
  // public ClojureSet without(Object key) { return disjoin(key); }
  // public ClojureSet toTransient() { return (ClojureSet) asTransient(); }
  // public ClojureSet toPersistent() { return (ClojureSet) persistent(); }

  // ===== LEAF =====

  static class Leaf {
    final Object[] _keys;
    int _len;
    final Edit _edit;

    Leaf(Object[] keys, int len, Edit edit) {
      _keys = keys;
      _len  = len;
      _edit = edit;
    }

    Object maxKey() {
      return _keys[_len-1];
    }

    Leaf newLeaf(int len, Edit edit) {
      if (edit.editable())
        return new Leaf(new Object[Math.min(MAX_LEN, len + EXTRA_LEN)], len, edit);
      else
        return new Leaf(new Object[len], len, edit);
    }

    int search(Object key, Comparator cmp) {
      int low = 0, high = _len;
      while (high - low > 16) {
        int mid = (high + low) >>> 1;
        int d = cmp.compare(_keys[mid], key);
        if (d == 0) return mid;
        else if (d > 0) high = mid;
        else low = mid;
      }

      // linear search
      for(int i = low; i < high; ++i) {
        final int d = cmp.compare(_keys[i], key);
        if (d == 0) return i;
        else if (d > 0) return -i-1;
      }
      return -high-1;
    }


    boolean contains(Object key, Comparator cmp) {
      return search(key, cmp) >= 0;
    }

    Leaf[] add(Object key, Comparator cmp, Edit edit) {
      int idx = search(key, cmp);
      if (idx >= 0) // already in set
        return UNCHANGED;
      
      int ins = -idx-1;

      // modifying array in place
      if (_edit.editable() && _len < _keys.length) {
        if (ins == _len) {
          _keys[_len++] = key;
          return new Leaf[]{this};
        } else {
          Stitch.copy(_keys, ins, _len, _keys, ins+1);
          _keys[ins] = key;
          ++_len;
          return EARLY_EXIT;
        }
      }

      // simply adding to array
      if (_len < MAX_LEN) {
        Leaf n = newLeaf(_len+1, edit);
        new Stitch(n._keys, 0)
          .copyAll(_keys, 0, ins)
          .copyOne(key)
          .copyAll(_keys, ins, _len);
        return new Leaf[]{n};
      }

      // splitting
      int half1 = (_len+1) >>> 1,
          half2 = _len+1-half1;

      // goes to first half
      if (ins < half1) {
        Leaf n1 = newLeaf(half1, edit),
             n2 = newLeaf(half2, edit);
        new Stitch(n1._keys, 0)
          .copyAll(_keys, 0, ins)
          .copyOne(key)
          .copyAll(_keys, ins, half1-1);
        Stitch.copy(_keys, half1-1, _len, n2._keys, 0);
        return new Leaf[]{n1, n2};
      }

      // copy first, insert to second
      Leaf n1 = newLeaf(half1, edit),
           n2 = newLeaf(half2, edit);
      Stitch.copy(_keys, 0, half1, n1._keys, 0);
      new Stitch(n2._keys, 0)
        .copyAll(_keys, half1, ins)
        .copyOne(key)
        .copyAll(_keys, ins, _len);
      return new Leaf[]{n1, n2};
    }

    Leaf[] remove(Object key, Leaf left, Leaf right, Comparator cmp, Edit edit) {
      int idx = search(key, cmp);
      if (idx < 0) // not in set
        return UNCHANGED;

      int newLen = _len-1;

      // nothing to merge
      if (newLen >= MIN_LEN || (left == null && right == null)) {

        // transient, can edit in place
        if (_edit.editable()) {
          Stitch.copy(_keys, idx+1, _len, _keys, idx);
          _len = newLen;
          if (idx == newLen) // removed last, need to signal new maxKey
            return new Leaf[]{left, this, right};
          return EARLY_EXIT;        
        }

        // persistent
        Leaf center = newLeaf(newLen, edit);
        new Stitch(center._keys, 0) 
          .copyAll(_keys, 0, idx)
          .copyAll(_keys, idx+1, _len);
        return new Leaf[] { left, center, right };
      }

      // can join with left
      if (left != null && left._len + newLen <= MAX_LEN) {
        Leaf join = newLeaf(left._len + newLen, edit);
        new Stitch(join._keys, 0)
          .copyAll(left._keys, 0,     left._len)
          .copyAll(_keys,      0,     idx)
          .copyAll(_keys,      idx+1, _len);
        return new Leaf[] { null, join, right };
      }
      
      // can join with right
      if (right != null && newLen + right._len <= MAX_LEN) {
        Leaf join = newLeaf(newLen + right._len, edit);
        new Stitch(join._keys, 0)
          .copyAll(_keys,       0,     idx)
          .copyAll(_keys,       idx+1, _len)
          .copyAll(right._keys, 0,     right._len);
        return new Leaf[]{ left, join, null };
      }

      // borrow from left
      if (left != null && (left._edit.editable() || right == null || left._len >= right._len)) {
        int totalLen     = left._len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen,
            leftTail     = left._len - newLeftLen;

        Leaf newLeft, newCenter;

        // prepend to center
        if (_edit.editable() && newCenterLen <= _keys.length) {
          newCenter = this;
          Stitch.copy(_keys,      idx+1,      _len,      _keys, leftTail + idx);
          Stitch.copy(_keys,      0,          idx,      _keys, leftTail);
          Stitch.copy(left._keys, newLeftLen, left._len, _keys, 0);
          _len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, edit);
          new Stitch(newCenter._keys, 0)
            .copyAll(left._keys, newLeftLen, left._len)
            .copyAll(_keys,      0,          idx)
            .copyAll(_keys,      idx+1,      _len);
        }

        // shrink left
        if (left._edit.editable()) {
          newLeft  = left;
          left._len = newLeftLen;
        } else {
          newLeft = newLeaf(newLeftLen, edit);
          Stitch.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
        }

        return new Leaf[]{ newLeft, newCenter, right };
      }

      // borrow from right
      if (right != null) {
        int totalLen     = newLen + right._len,
            newCenterLen = totalLen >>> 1,
            newRightLen  = totalLen - newCenterLen,
            rightHead    = right._len - newRightLen;
        
        Leaf newCenter, newRight;
        
        // append to center
        if (_edit.editable() && newCenterLen <= _keys.length) {
          newCenter = this;
          new Stitch(_keys, idx)
            .copyAll(_keys,       idx+1, _len)
            .copyAll(right._keys, 0,     rightHead);
          _len = newCenterLen;
        } else {
          newCenter = newLeaf(newCenterLen, edit);
          new Stitch(newCenter._keys, 0)
            .copyAll(_keys,       0,     idx)
            .copyAll(_keys,       idx+1, _len)
            .copyAll(right._keys, 0,     rightHead);
        }

        // cut head from right
        if (right._edit.editable()) {
          newRight = right;
          Stitch.copy(right._keys, rightHead, right._len, right._keys, 0);
          right._len = newRightLen;
        } else {
          newRight = newLeaf(newRightLen, edit);
          Stitch.copy(right._keys, rightHead, right._len, newRight._keys, 0);
        }

        return new Leaf[]{ left, newCenter, newRight };
      }
      throw new RuntimeException("Unreachable");
    }

    public String str(int lvl) {
      StringBuilder sb = new StringBuilder("{");
      for (int i = 0; i < _len; ++i) {
        if (i > 0) sb.append(" ");
        sb.append(_keys[i].toString());
      }
      return sb.append("}").toString();
    }
  }


  // ===== NODE =====

  static class Node extends Leaf {
    final Leaf[] _children;
    
    Node(Object[] keys, Leaf[] children, int len, Edit edit) {
      super(keys, len, edit);
      _children = children;
    }

    Node newNode(int len, Edit edit) {
      return new Node(new Object[len], new Leaf[len], len, edit);
    }

    boolean contains(Object key, Comparator cmp) {
      int idx = search(key, cmp);
      if (idx >= 0) return true;
      int ins = -idx-1; 
      if (ins == _len) return false;
      return _children[ins].contains(key, cmp);
    }

    Leaf[] add(Object key, Comparator cmp, Edit edit) {
      int idx = search(key, cmp);
      if (idx >= 0) // already in set
        return UNCHANGED;
      
      int ins = -idx-1;
      if (ins == _len) ins = _len-1;
      Leaf[] nodes = _children[ins].add(key, cmp, edit);

      if (UNCHANGED == nodes) // child signalling already in set
        return UNCHANGED;

      if (EARLY_EXIT == nodes) // child signalling nothing to update
        return EARLY_EXIT;
      
      // same len
      if (1 == nodes.length) {
        Leaf node = nodes[0];
        if (_edit.editable()) {
          _keys[ins] = node.maxKey();
          _children[ins] = node;
          return ins==_len-1 && node.maxKey() == maxKey() ? new Leaf[]{this} : EARLY_EXIT;
        }

        Object[] newKeys;
        if (0 == cmp.compare(node.maxKey(), _keys[ins]))
          newKeys = _keys;
        else {
          newKeys = Arrays.copyOfRange(_keys, 0, _len);
          newKeys[ins] = node.maxKey();
        }

        Leaf[] newChildren;
        if (node == _children[ins])
          newChildren = _children;
        else {
          newChildren = Arrays.copyOfRange(_children, 0, _len);
          newChildren[ins] = node;
        }

        return new Leaf[]{new Node(newKeys, newChildren, _len, edit)};
      }

      // len + 1
      if (_len < MAX_LEN) {
        Node n = newNode(_len+1, edit);
        new Stitch(n._keys, 0)
          .copyAll(_keys, 0, ins)
          .copyOne(nodes[0].maxKey())
          .copyOne(nodes[1].maxKey())
          .copyAll(_keys, ins+1, _len);

        new Stitch(n._children, 0)
          .copyAll(_children, 0, ins)
          .copyOne(nodes[0])
          .copyOne(nodes[1])
          .copyAll(_children, ins+1, _len);
        return new Leaf[]{n};
      }

      // split
      int half1 = (_len+1) >>> 1;
      if (ins+1 == half1) ++half1;
      int half2 = _len+1-half1;

      // add to first half
      if (ins < half1) {
        Object keys1[] = new Object[half1];
        new Stitch(keys1, 0)
          .copyAll(_keys, 0, ins)
          .copyOne(nodes[0].maxKey())
          .copyOne(nodes[1].maxKey())
          .copyAll(_keys, ins+1, half1-1);
        Object keys2[] = new Object[half2];
        Stitch.copy(_keys, half1-1, _len, keys2, 0);

        Leaf children1[] = new Leaf[half1];
        new Stitch(children1, 0)
          .copyAll(_children, 0, ins)
          .copyOne(nodes[0])
          .copyOne(nodes[1])
          .copyAll(_children, ins+1, half1-1);
        Leaf children2[] = new Leaf[half2];
        Stitch.copy(_children, half1-1, _len, children2, 0);
        return new Leaf[]{new Node(keys1, children1, half1, edit),
                          new Node(keys2, children2, half2, edit)};
      }

      // add to second half
      Object keys1[] = new Object[half1],
             keys2[] = new Object[half2];
      Stitch.copy(_keys, 0, half1, keys1, 0);

      new Stitch(keys2, 0)
        .copyAll(_keys, half1, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins+1, _len);

      Leaf children1[] = new Leaf[half1],
           children2[] = new Leaf[half2];
      Stitch.copy(_children, 0, half1, children1, 0);

      new Stitch(children2, 0)
        .copyAll(_children, half1, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins+1, _len);
      return new Leaf[]{new Node(keys1, children1, half1, edit),
                        new Node(keys2, children2, half2, edit)};
    }

    Leaf[] remove(Object key, Leaf left, Leaf right, Comparator cmp, Edit edit) {
      return remove(key, (Node) left, (Node) right, cmp, edit);
    }

    Leaf[] remove(Object key, Node left, Node right, Comparator cmp, Edit edit) {
      int idx = search(key, cmp);
      if (idx < 0) idx = -idx-1;

      if (idx == _len) // not in set
        return UNCHANGED;
      
      Leaf leftChild  = idx > 0      ? _children[idx-1] : null,
           rightChild = idx < _len-1 ? _children[idx+1] : null;
      Leaf[] nodes = _children[idx].remove(key, leftChild, rightChild, cmp, edit);

      if (UNCHANGED == nodes) // child signalling element not in set
        return UNCHANGED;

      if (EARLY_EXIT == nodes) // child signalling nothing to update
        return EARLY_EXIT;

      // nodes[1] always not nil
      int newLen = _len - 1
                   - (leftChild  != null ? 1 : 0)
                   - (rightChild != null ? 1 : 0)
                   + (nodes[0] != null ? 1 : 0)
                   + 1
                   + (nodes[2] != null ? 1 : 0);

      // no rebalance needed
      if (newLen >= MIN_LEN || (left == null && right == null)) {
        // can update in place
        if (_edit.editable() && idx < _len-2) {
          Stitch<Object> ks = new Stitch(_keys, Math.max(idx-1, 0));
          if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                                ks.copyOne(nodes[1].maxKey());
          if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
          if (newLen != _len)
            ks.copyAll(_keys, idx+2, _len);

          Stitch<Leaf> cs = new Stitch(_children, Math.max(idx-1, 0));
          if (nodes[0] != null) cs.copyOne(nodes[0]);
                                cs.copyOne(nodes[1]);
          if (nodes[2] != null) cs.copyOne(nodes[2]);
          if (newLen != _len)
            cs.copyAll(_children, idx+2, _len);

          _len = newLen;
          return EARLY_EXIT;
        }

        Node newCenter = newNode(newLen, edit);

        Stitch<Object> ks = new Stitch(newCenter._keys, 0);
        ks.copyAll(_keys, 0, idx-1);
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        ks.copyAll(_keys, idx+2, _len);

        Stitch<Leaf> cs = new Stitch(newCenter._children, 0);
        cs.copyAll(_children, 0, idx-1);
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        cs.copyAll(_children, idx+2, _len);

        return new Leaf[] { left, newCenter, right };
      }

      // can join with left
      if (left != null && left._len + newLen <= MAX_LEN) {
        Node join = newNode(left._len + newLen, edit);

        Stitch<Object> ks = new Stitch(join._keys, 0);
        ks.copyAll(left._keys, 0, left._len);
        ks.copyAll(_keys,      0, idx-1);
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        ks.copyAll(_keys,     idx+2, _len);

        Stitch<Leaf> cs = new Stitch(join._children, 0);
        cs.copyAll(left._children, 0, left._len);
        cs.copyAll(_children,      0, idx-1);
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        cs.copyAll(_children, idx+2, _len);

        return new Leaf[] { null, join, right };
      }

      // can join with right
      if (right != null && newLen + right._len <= MAX_LEN) {
        Node join = newNode(newLen + right._len, edit);

        Stitch<Object> ks = new Stitch(join._keys, 0);
        ks.copyAll(_keys, 0, idx-1);
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        ks.copyAll(_keys,       idx+2, _len);
        ks.copyAll(right._keys, 0, right._len);

        Stitch<Leaf> cs = new Stitch(join._children, 0);
        cs.copyAll(_children, 0, idx-1);
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        cs.copyAll(_children,     idx+2, _len);
        cs.copyAll(right._children, 0, right._len);
        
        return new Leaf[] { left, join, null };
      }

      // borrow from left
      if (left != null && (right == null || left._len >= right._len)) {
        int totalLen     = left._len + newLen,
            newLeftLen   = totalLen >>> 1,
            newCenterLen = totalLen - newLeftLen;

        Node newLeft   = newNode(newLeftLen,   edit),
             newCenter = newNode(newCenterLen, edit);

        Stitch.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

        Stitch<Object> ks = new Stitch(newCenter._keys, 0);
        ks.copyAll(left._keys, newLeftLen, left._len);
        ks.copyAll(_keys, 0, idx-1);
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        ks.copyAll(_keys, idx+2, _len);

        Stitch.copy(left._children, 0, newLeftLen, newLeft._children, 0);

        Stitch<Leaf> cs = new Stitch(newCenter._children, 0);
        cs.copyAll(left._children, newLeftLen, left._len);
        cs.copyAll(_children, 0, idx-1);
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        cs.copyAll(_children, idx+2, _len);

        return new Leaf[] { newLeft, newCenter, right };
      }

      // borrow from right
      if (right != null) {
        int totalLen     = newLen + right._len,
            newCenterLen = totalLen >>> 1,
            newRightLen  = totalLen - newCenterLen,
            rightHead    = right._len - newRightLen;

        Node newCenter = newNode(newCenterLen, edit),
             newRight  = newNode(newRightLen,  edit);

        Stitch<Object> ks = new Stitch(newCenter._keys, 0);
        ks.copyAll(_keys, 0, idx-1);
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        ks.copyAll(_keys, idx+2, _len);
        ks.copyAll(right._keys, 0, rightHead);

        Stitch.copy(right._keys, rightHead, right._len, newRight._keys, 0);

        Stitch<Object> cs = new Stitch(newCenter._children, 0);
        cs.copyAll(_children, 0, idx-1);
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        cs.copyAll(_children, idx+2, _len);
        cs.copyAll(right._children, 0, rightHead);

        Stitch.copy(right._children, rightHead, right._len, newRight._children, 0);        

        return new Leaf[] { left, newCenter, newRight };
      }

      throw new RuntimeException("Unreachable");
    }

    public String str(int lvl) {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i < _len; ++i) {
        sb.append("\n");
        for (int j=0; j < lvl; ++j)
          sb.append("| ");
        sb.append(_keys[i] + ": " + _children[i].str(lvl+1));
      }
      return sb.toString();
    }
  }


  // ===== ITER =====

  static class JavaIter implements Iterator {
    final Seq _seq;
    boolean _over;

    JavaIter(Seq seq) {
      _seq = seq;
      _over = seq == null;
    }
    public boolean hasNext() { return !_over; }
    public Object next() {
      Object res = _seq.first();
      _over = false == _seq.advance();
      return res;
    }
  }

  // ===== SEQ =====

  static class Seq extends ASeq implements IReduce {
    Seq  _parent;
    Leaf _node;
    int  _idx;
    final Object _keyTo;
    final Comparator _cmp;

    Seq(IPersistentMap meta, Seq parent, Leaf node, int idx, Object keyTo, Comparator cmp) {
      _parent = parent;
      _node   = node;
      _idx    = idx;
      _keyTo  = keyTo;
      _cmp    = cmp;
    }

    Leaf child() {
      assert _node instanceof Node;
      return ((Node) _node)._children[_idx];
    }

    boolean advance() {
      if (_idx < _node._len-1) {
        _idx++;
        return _keyTo == null || _cmp.compare(first(), _keyTo) <= 0;
      } else if (_parent != null) {
        if (_parent.advance()) {
          _node = _parent.child();
          _idx = 0;
          return _keyTo == null || _cmp.compare(first(), _keyTo) <= 0;
        }
      }
      return false;
    }

    // ASeq
    public Object first() {
      assert !(_node instanceof Node);
      return _node._keys[_idx];
    }

    public Seq next() {
      Seq res = null;
      if (_idx < _node._len-1)
        res = new Seq(meta(), _parent, _node, _idx+1, _keyTo, _cmp);
      else if (_parent != null) {
          Seq nextParent = _parent.next();
          if (nextParent != null)
            res = new Seq(meta(), nextParent, nextParent.child(), 0, _keyTo, _cmp);
        }
      if (res != null && (_keyTo == null || _cmp.compare(res.first(), _keyTo) <= 0))
        return res;
      return null;
    }

    public Obj withMeta(IPersistentMap meta) {
      if(meta() == meta) return this;
      return new Seq(meta, _parent, _node, _idx, _keyTo, _cmp);
    }

    // IReduce
    public Object reduce(IFn f) {
      Object ret = first();
      while (advance()) {
        ret = f.invoke(ret, first());
        if (ret instanceof Reduced)
          return ((Reduced) ret).deref();
      }
      return ret;
    }

    public Object reduce(IFn f, Object start) {
      Object ret = start;
      do {
        ret = f.invoke(ret, first());
        if (ret instanceof Reduced)
          return ((Reduced) ret).deref();
      } while (advance());
      return ret;
    }
  }
}