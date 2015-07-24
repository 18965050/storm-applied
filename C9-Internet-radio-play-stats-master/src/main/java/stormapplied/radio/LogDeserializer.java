package stormapplied.radio;

import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogDeserializer extends BaseFunction {
  private transient Gson gson;

  @Override
  public void execute(TridentTuple tuple,
                      TridentCollector collector) {
    String logLine = tuple.getString(0);
    LogEntry logEntry = gson.fromJson(logLine, LogEntry.class);
    if (logEntry != null) {
      collector.emit(new Values(logEntry.getArtist(),
                                logEntry.getTitle(),
                                logEntry.getTags()));
    }
  }

  @Override
  public void prepare(Map config,
                      TridentOperationContext context) {
    gson = new Gson();
  }

  @Override
  public void cleanup() { }

  public static class LogEntry {
    private String artist;
    private String title;
    private List<String> tags = new ArrayList<String>();

    public String getArtist() { return artist; }

    public void setArtist(String artist) { this.artist = artist; }

    public String getTitle() { return title; }

    public void setTitle(String title) { this.title = title; }

    public List<String> getTags() { return tags; }

    public void setTags(List<String> tags) { this.tags = tags; }
  }
}