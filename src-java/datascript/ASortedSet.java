package datascript;

import java.util.*;
import clojure.lang.*;

//  clojure.lang.Reversible
//  (rseq [this] (rseq (btset-iter this)))

@SuppressWarnings("unchecked")
public abstract class ASortedSet implements IObj, IPersistentSet, ILookup, Iterable, ISortedSet, Set, java.io.Serializable, IHashEq {
  int _hash;
  int _hasheq;
  final IPersistentMap _meta;
  final Comparator _cmp;

  public ASortedSet(IPersistentMap meta, Comparator cmp) {
    _meta = meta;
    _cmp = cmp;
  }

  // IMeta
  public IPersistentMap meta() { return _meta; }

  // IObj
  abstract public ASortedSet withMeta(IPersistentMap meta);

  // Counted
  abstract public int count();

  //  Seqable
  abstract public ISeq seq();

  // IPersistentCollection
  abstract public ASortedSet empty();
  abstract public ASortedSet cons(Object key);

  public boolean equiv(Object obj) {
    if (!(obj instanceof Set)) return false;
    Set s = (Set) obj;
    if (s.size() != count()) return false;
    return containsAll(s);
  }

  // IPersistentSet
  abstract public ASortedSet disjoin(Object key);
  abstract public boolean contains(Object o);
  public Object get(Object key) { return contains(key) ? key : null; }
     
  //  ILookup
  public Object valAt(Object key) { return contains(key) ? key : null; }
  public Object valAt(Object key, Object notFound) { return contains(key) ? key : notFound; }

  // IFn
  public Object invoke(Object key) { return contains(key) ? key : null; }
  public Object invoke(Object key, Object notFound) { return contains(key) ? key : notFound; }

  // IHashEq
  public int hasheq() {
    if (_hasheq == 0)
      _hasheq = Murmur3.hashUnordered(this);
    return _hasheq;
  }

  // Iterable
  abstract public Iterator iterator();

  // Collection
  public boolean containsAll​(Collection c) {
    for (Object o: c)
      if (!contains(o))
        return false;
    return true;
  }
  public int      size()                  { return count(); }
  public boolean  isEmpty()               { return count() == 0; }
  public Object[] toArray()               { return RT.seqToArray(seq()); }
  public Object[] toArray(Object[] arr)   { return RT.seqToPassedArray(seq(), arr); }
  public boolean  add(Object o)           { throw new UnsupportedOperationException(); }
  public boolean  remove(Object o)        { throw new UnsupportedOperationException(); }
  public boolean  addAll(Collection c)    { throw new UnsupportedOperationException(); }
  public void     clear()                 { throw new UnsupportedOperationException(); }
  public boolean  retainAll(Collection c) { throw new UnsupportedOperationException(); }
  public boolean  removeAll(Collection c) { throw new UnsupportedOperationException(); }

  // Object
  public int hashCode() {
    int hash = _hash;
    if (hash == 0) {
      for (Object o: this)
        hash += Util.hash(o);
      _hash = hash;
    }
    return hash;
  }

  public boolean equals(Object obj) {
    return equiv(obj);
  }

  public String toString() {
    return RT.printString(this);
  }
}