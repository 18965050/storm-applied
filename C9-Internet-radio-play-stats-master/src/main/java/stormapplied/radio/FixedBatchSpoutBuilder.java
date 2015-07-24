package stormapplied.radio;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.testing.FixedBatchSpout;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FixedBatchSpoutBuilder {
  public static FixedBatchSpout buildSpout() {
    List<Values> playLogsData = new FixedBatchSpoutBuilder().readData();
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("play-log"), 5, playLogsData.toArray(new Values[playLogsData.size()]));
    spout.setCycle(true);
    return spout;
  }

  public List<Values> readData() {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/playlogs.txt")));

      List<Values> playLogsData = new ArrayList<Values>();
      String line = null;
      while ((line = reader.readLine()) != null) playLogsData.add(new Values(line));

      return playLogsData;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
