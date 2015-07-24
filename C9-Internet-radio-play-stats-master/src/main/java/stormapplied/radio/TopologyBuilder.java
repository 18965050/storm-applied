package stormapplied.radio;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.ILocalDRPC;
import org.apache.thrift7.TException;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import static stormapplied.radio.FixedBatchSpoutBuilder.buildSpout;

public class TopologyBuilder {
  public static StormTopology buildLocal(ILocalDRPC drpc) {
    return build(drpc);
  }

  public static StormTopology buildRemote() {
    return build(null);
  }  

  private static StormTopology build(ILocalDRPC drpc) {
    TridentTopology topology = new TridentTopology();

    Stream playStream =
      topology.newStream("play-spout", buildSpout())
              .each(new Fields("play-log"),
                    new LogDeserializer(),
                    new Fields("artist", "title", "tags"))
              .each(new Fields("artist", "title"),
                    new Sanitizer(new Fields("artist", "title")))
              .name("LogDeserializerSanitizer");

    TridentState countByArtist = playStream
      .project(new Fields("artist"))
      .groupBy(new Fields("artist"))
      .name("ArtistCounts")
      .persistentAggregate(new MemoryMapState.Factory(),
                           new Count(),
                           new Fields("artist-count"))
      .parallelismHint(4);

    TridentState countsByTitle = playStream
      .project(new Fields("title"))
      .groupBy(new Fields("title"))
      .name("TitleCounts")
      .persistentAggregate(new MemoryMapState.Factory(),
                           new Count(),
                           new Fields("title-count"))
      .parallelismHint(4);

    TridentState countsByTag = playStream
      .each(new Fields("tags"),
            new ListSplitter(),
            new Fields("tag"))
      .project(new Fields("tag"))
      .groupBy(new Fields("tag"))
      .name("TagCounts")
      .persistentAggregate(new MemoryMapState.Factory(),
                           new Count(),
                           new Fields("tag-count"))
      .parallelismHint(4);

    topology.newDRPCStream("count-request-by-tag", drpc)
            .name("RequestForTagCounts")
            .each(new Fields("args"),
                  new SplitOnDelimiter(","),
                  new Fields("tag"))
            .groupBy(new Fields("tag"))
            .name("QueryForRequest")
            .stateQuery(countsByTag,
                        new Fields("tag"),
                        new MapGet(),
                        new Fields("count"));

    return topology.build();
  }
}
