package datascript;

import java.util.*;
import clojure.lang.*;

public interface ISortedSet extends Seqable, Reversible, Sorted {
  ISeq slice(Object from, Object to, Comparator cmp);
  ISeq rslice(Object from, Object to, Comparator cmp);

  default ISeq slice(Object from, Object to) { return slice(from, to, comparator()); }
  default ISeq rslice(Object from, Object to) { return rslice(from, to, comparator()); }

  //  Seqable
  default ISeq seq() { return slice(null, null, comparator()); }

  // Reversible
  default ISeq rseq() { return rslice(null, null, comparator()); }

  // Sorted
  default ISeq seq(boolean asc) { return asc ? slice(null, null, comparator()) : rslice(null, null, comparator()); }
  default ISeq seqFrom(Object key, boolean asc) { return asc ? slice(key, null, comparator()) : rslice(key, null, comparator()); }
}