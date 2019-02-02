package datascript;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

/**
* Java rewrite of Clojure btset.clj impl
*/

@SuppressWarnings("unchecked")
public class PersistentBTSet implements ISortedSet {
  Comparator cmp;
  Node root;
  int minLen = 64;
  int maxLen = 128;

  public PersistentBTSet() { this(Comparator.naturalOrder()); }

  public PersistentBTSet(int maxLen) { 
    this(Comparator.naturalOrder());
    this.maxLen = maxLen;
    this.minLen = maxLen / 2;
  }

  public PersistentBTSet(Comparator cmp) {
    this.cmp = cmp;
    root = new Node(new Object[]{}, null, 0);
  }

  PersistentBTSet(Comparator cmp, Node root, int minLen, int maxLen) {
    this.cmp = cmp;
    this.root = root;
    this.minLen = minLen;
    this.maxLen = maxLen;
  }

  class Node {
    Object[] keys;
    Node[] children;
    int len;

    Node(Object[] keys, Node[] children, int len) {
      this.keys = keys;
      this.children = children;
      this.len = len;
    }

    Object maxKey() {
      return keys[len-1];
    }

    Node[] add(Object key) {
      if(null == children) { // leaf
        int idx = Arrays.binarySearch(keys, 0, len, key, cmp);
        if (idx >= 0) // already in set
          return null;
        
        int ins = -idx - 1;
        if (len < maxLen) { // simply adding to array
          Node n = new Node(new Object[len+1], null, len+1);
          System.arraycopy(keys, 0, n.keys, 0, ins);
          n.keys[ins] = key;
          System.arraycopy(keys, ins, n.keys, ins+1, len-ins);
          return new Node[]{n};
        }
        // splitting
        int half1 = (len+1) >> 1,
            half2 = len+1-half1;
        Node n1 = new Node(new Object[half1], null, half1),
             n2 = new Node(new Object[half2], null, half2);
        if (ins < half1) {
          System.arraycopy(keys, 0, n1.keys, 0, ins);
          n1.keys[ins] = key;
          System.arraycopy(keys, ins, n1.keys, ins+1, half1-ins-1);
          System.arraycopy(keys, half1-1, n2.keys, 0, half2);
        } else {
          System.arraycopy(keys, 0, n1.keys, 0, half1);
          System.arraycopy(keys, half1, n2.keys, 0, ins-half1);
          n2.keys[ins-half1] = key;
          System.arraycopy(keys, ins, n2.keys, ins-half1+1, len-ins);
        }
        return new Node[]{n1, n2};
      }


      // not leaf
      int idx = Arrays.binarySearch(keys, 0, len-1, key, cmp);
      if (idx >= 0) // already in set
        return null;
      
      int ins = -idx - 1;
      Node[] nodes = children[ins].add(key);
      if (null == nodes)
        return null;
      
      // same len
      if (nodes.length == 1) {
        Object[] newKeys;
        if (0 == cmp.compare(nodes[0].maxKey(), keys[ins]))
          newKeys = keys;
        else {
          newKeys = Arrays.copyOfRange(keys, 0, len);
          newKeys[ins] = nodes[0].maxKey();
        }

        Node[] newChildren = Arrays.copyOfRange(children, 0, len);
        newChildren[ins] = nodes[0];
        return new Node[]{new Node(newKeys, newChildren, len)};
      }

      // len + 1
      if (len < maxLen) {
        Node n = new Node(new Object[len+1], new Node[len+1], len+1);
        System.arraycopy(keys, 0, n.keys, 0, ins);
        n.keys[ins] = nodes[0].maxKey();
        n.keys[ins+1] = nodes[1].maxKey();
        System.arraycopy(keys, ins+1, n.keys, ins+2, len-ins-1);

        System.arraycopy(children, 0, n.children, 0, ins);
        n.children[ins] = nodes[0];
        n.children[ins+1] = nodes[1];
        System.arraycopy(children, ins+1, n.children, ins+2, len-ins-1);
        return new Node[]{n};
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

        Node children1[] = new Node[half1];
        System.arraycopy(children, 0, children1, 0, ins);
        children1[ins] = nodes[0];
        children1[ins+1] = nodes[1];
        System.arraycopy(children, ins+1, children1, ins+2, half1-ins-2);
        Node children2[] = Arrays.copyOfRange(children, half1-1, len);
        return new Node[]{new Node(keys1, children1, half1),
                          new Node(keys2, children2, half2)};
      }

      // add to second half
      Object keys1[] = Arrays.copyOfRange(keys, 0, half1);
      Object keys2[] = new Object[half2];
      System.arraycopy(keys, half1, keys2, 0, ins-half1);
      keys2[ins-half1] = nodes[0].maxKey();
      keys2[ins-half1+1] = nodes[1].maxKey();
      System.arraycopy(keys, ins+1, keys2, ins-half1+2, len-ins-1);

      Node children1[] = Arrays.copyOfRange(children, 0, half1);
      Node children2[] = new Node[half2];
      System.arraycopy(children, half1, children2, 0, ins-half1);
      children2[ins-half1] = nodes[0];
      children2[ins-half1+1] = nodes[1];
      System.arraycopy(children, ins+1, children2, ins-half1+2, len-ins-1); 
      return new Node[]{new Node(keys1, children1, half1),
                        new Node(keys2, children2, half2)};
    }

    public String toString() {
      if (null == children) {
        String res = "";
        for (int i=0; i<len; ++i)
          res += keys[i].toString() + ", ";
        return res;
      } else {
        String res = "";
        for (int i=0; i<len; ++i)
          res += children[i].toString();
        return res;
      }
    }
  }

  public PersistentBTSet with(Object key) {
    Node nodes[] = root.add(key);
    if (null == nodes)
      return this;
    if (1 == nodes.length)
      return new PersistentBTSet(cmp, nodes[0], minLen, maxLen);
    else {
      Object keys[] = new Object[]{nodes[0].maxKey(), nodes[1].maxKey()};
      Node newRoot = new Node(keys, nodes, 2);
      return new PersistentBTSet(cmp, newRoot, minLen, maxLen);
    }
  }

  public String toString() {
    return "#{" + root.toString() + "}";
  }
}