package com.purbon.kstreams;

import com.purbon.kstreams.processors.CountingTransformer;
import com.purbon.kstreams.eks.ElasticsearchStateStore;
import com.purbon.kstreams.eks.ElasticsearchStoreBuilder;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;

public class App {

  public static Properties configure() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }
  public static void main(String[] args) throws Exception {

    /*
      // Here using the processor API
      Topology topology = new Topology();
      ElasticsearchStoreBuilder storeSupplier = new ElasticsearchStoreBuilder();

      topology.addSource("Source", "source-topic")
          .addProcessor("Process", () -> new CountingProcessor(), "Source")
          .addStateStore(storeSupplier, "Process")
          .addSink("Sink", "target-topic", "Process");
    */


    // Using the Stream DSL and the Transformer API
    StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(new ElasticsearchStoreBuilder());
    builder
        .stream("source-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .transform(CountingTransformer::new, ElasticsearchStateStore.STORE_NAME)
        .to("target-topic");

    final KafkaStreams streams = new KafkaStreams(builder.build(), configure());
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
