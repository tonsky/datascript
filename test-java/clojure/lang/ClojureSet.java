package clojure.lang;

import java.util.*;
import clojure.java.api.Clojure;

@SuppressWarnings("unchecked")
public class ClojureSet extends PersistentTreeSet implements datascript.ISortedSet {
  static public final ClojureSet EMPTY = new ClojureSet(null, PersistentTreeMap.EMPTY);

  static IFn takeWhile;
  static {
    takeWhile = (IFn) Clojure.var("clojure.core", "take-while");
  }

  ClojureSet(IPersistentMap meta, IPersistentMap impl) {
    super(meta, impl);
  }

  public IPersistentSet cons(Object o) {
    if(contains(o))
      return this;
    return new ClojureSet(meta(), impl.assoc(o,o));
  }

  public ISeq slice(Object from, Object to, Comparator cmp) {
    IFn pred = new AFn() {
      public Object invoke(Object arg) {
        return cmp.compare(arg, to) <= 0; 
      }
    };
    return (ISeq) takeWhile.invoke(pred, seqFrom(from, true));
  }

  public ISeq rslice(Object from, Object to, Comparator cmp) {
    IFn pred = new AFn() {
      public Object invoke(Object arg) {
        return cmp.compare(arg, to) >= 0; 
      }
    };
    return (ISeq) takeWhile.invoke(pred, seqFrom(from, false));
  }
}
