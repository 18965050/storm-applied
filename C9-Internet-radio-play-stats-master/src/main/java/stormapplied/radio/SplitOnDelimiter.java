package stormapplied.radio;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitOnDelimiter extends BaseFunction {
  private final String delimiter;

  public SplitOnDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  @Override
  public void execute(TridentTuple tuple,
                      TridentCollector collector) {
    for (String part : tuple.getString(0).split(delimiter)) {
      if (part.length() > 0) collector.emit(new Values(part));
    }
  }
}