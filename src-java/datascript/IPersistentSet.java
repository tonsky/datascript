package datascript;

public interface IPersistentSet {
  IPersistentSet add(Object o);
  default int size() { return 0; }
}