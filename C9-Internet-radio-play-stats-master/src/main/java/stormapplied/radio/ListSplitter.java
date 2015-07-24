package stormapplied.radio;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import java.util.List;

public class ListSplitter extends BaseFunction {
  @Override
  public void execute(TridentTuple tuple,
                      TridentCollector collector) {
    List<?> list = (List<?>) tuple.getValue(0);
    for (Object o : list) {
      collector.emit(new Values(o));
    }
  }
}
