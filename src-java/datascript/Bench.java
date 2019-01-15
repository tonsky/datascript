package datascript;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.LongStream;
import java.util.stream.Collectors;

interface ISetFactory {
  IPersistentSet create(int maxLen);
}

interface IBenchRunner {
  long run(IPersistentSet target);
}

public class Bench {
  public static long benchAdd(IPersistentSet target) {
    long t0 = System.currentTimeMillis();
    IPersistentSet result = target;
    for (Object o: source)
      result = result.add(o);
    return System.currentTimeMillis() - t0;
  }

  static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  // static Integer[] maxLens = new Integer[]{128};
  static int warmups = 10;
  static int runs = 10;

  static ArrayList<Long> source = new ArrayList<Long>();
  static {
    LongStream.range(0, 100000).forEach((long l) -> source.add(l));
    Collections.shuffle(source);
  }

  public static String range(Collection<Long> coll) {
    return "" + coll.stream().reduce(Long.MAX_VALUE, (Long a, Long b) -> a<b?a:b)
           + ".." + coll.stream().reduce(0L, (a,b) -> a<b?b:a) + "ms";
  }

  public static void runBench(String type, ISetFactory f, IBenchRunner r) {
    System.out.print(type + ": ");
    for(int maxLen: maxLens) {
      for(int i=0; i<warmups; ++i) {
        r.run(f.create(maxLen));
      }
      ArrayList<Long> measurments = new ArrayList<Long>();
      for(int i=0; i<runs; ++i) {
        measurments.add(Long.valueOf(r.run(f.create(maxLen))));
      }
      System.out.print("\t" + range(measurments));
    }
    System.out.println();
  }

  public static void main(String args[]) {
    System.out.println("Lengths:   \t" + String.join("\t", Arrays.stream(maxLens).map((ml)-> ml.toString() + " max    ").collect(Collectors.toList())));

    IBenchRunner r = (s) -> benchAdd(s);
    runBench("Persistent", (int ml)-> new PersistentBTSet(ml), r);
    runBench("Transient.tr", (int ml)-> new TransientBTSet(ml).asTransient(), r);
    runBench("TwoNodes.tr", (int ml)-> new TwoNodesSet(ml).asTransient(), r);
    runBench("ExtraLeaf", (int ml)-> new ExtraLeafSet(ml), r);
    runBench("ExtraLeaf.tr", (int ml)-> new ExtraLeafSet(ml).asTransient(), r);
    runBench("EarlyExit.tr", (int ml)-> new EarlyExitSet(ml).asTransient(), r);
  }
}