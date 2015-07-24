package stormapplied.radio;

import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import java.util.regex.Pattern;

public class Sanitizer extends BaseFilter {
  private final Pattern GARBAGE = Pattern.compile(".*test.*");
  private Fields inputFields;

  public Sanitizer(Fields inputFields) {
    this.inputFields = inputFields;
  }

  @Override
  public boolean isKeep(TridentTuple tuple) {
    for (String fieldName: inputFields.toList()) {
      String value = tuple.getStringByField(fieldName);
      if (GARBAGE.matcher(value).matches()) {
        return false;
      }
    }
    return true;
  }
}
