package datascript;

import java.util.Iterator;

public interface IPersistentSet extends Iterable {
  IPersistentSet add(Object o);
  default int size() { return 0; }
  default boolean contains(Object o) { throw new UnsupportedOperationException(); }
  default IPersistentSet asTransient() { return this; }
  default IPersistentSet persistent() { return this; }
  default Iterator iterator() { throw new UnsupportedOperationException(); }
}