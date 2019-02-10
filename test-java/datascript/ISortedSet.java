package datascript;

import java.util.*;
import clojure.lang.*;

public interface ISortedSet extends Iterable, Seqable {
  ISortedSet with(Object o);
  default ISortedSet without(Object o) { throw new UnsupportedOperationException(); }
  default int size() { return -1; }
  default boolean contains(Object o) { throw new UnsupportedOperationException(); }
  default ISortedSet toTransient() { throw new UnsupportedOperationException(); }
  default ISortedSet toPersistent() { throw new UnsupportedOperationException(); }
  default Iterator iterator() { throw new UnsupportedOperationException(); }
  default ISeq seq() { throw new UnsupportedOperationException(); }
}