import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.IsEqual.equalTo;

import com.purbon.kstreams.eks.Document;
import com.purbon.kstreams.eks.ElasticsearchStateStore;
import com.purbon.kstreams.eks.ElasticsearchStoreBuilder;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class ElasticsearchStoreTest {


  public static final String SOURCE_TOPIC = "source-topic";
  public static final String TARGET_TOPIC = "target-topic";
  private static ElasticsearchContainer container;


  private class TestTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ElasticsearchStateStore store;

    @Override
    public void init(ProcessorContext context) {
      this.store = (ElasticsearchStateStore) context.getStateStore(ElasticsearchStateStore.STORE_NAME);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {

      Document<String, String> doc = new Document<>();
      doc.docId = key;
      doc.content.put("title", value.toLowerCase());
      store.write(key, doc);
      return new KeyValue<>(key, value.toLowerCase());
    }

    @Override
    public void close() {

    }

  }

  @Before
  public void before() throws InterruptedException {
     container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:6.6.1");
     container.start();
     while(!container.isRunning()) {
        Thread.sleep(1);
     }
  }

  @After
  public void after() {
    container.stop();
  }

  private TopologyTestDriver buildTopologyDriver(TransformerSupplier<String, String, KeyValue<String, String>> transformerSupplier) {

    StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(new ElasticsearchStoreBuilder(container.getHttpHostAddress()));

    builder
        .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
        .transform(transformerSupplier, ElasticsearchStateStore.STORE_NAME)
        .to(TARGET_TOPIC);

    return new TopologyTestDriver(builder.build(), configure());
  }

  private Properties configure() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggrApp");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }

  @Test
  public void testStoreSetup() {

    TopologyTestDriver driver = buildTopologyDriver(TestTransformer::new);

    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(SOURCE_TOPIC, new StringSerializer(), new StringSerializer());

    driver.pipeInput(factory.create(SOURCE_TOPIC, "driver", "this is a value for the DRIVER"));
    driver.pipeInput(factory.create(SOURCE_TOPIC, "streams", "kafka streams is EASY"));

    ElasticsearchStateStore store = (ElasticsearchStateStore)driver.getStateStore(ElasticsearchStateStore.STORE_NAME);

    Document<String, String> doc = store.read("driver");
    Assert.assertThat(doc.content.get("title"), equalTo("this is a value for the driver"));

    driver.close();
  }

  @Test
  public void testStoreSearch() {
    TopologyTestDriver driver = buildTopologyDriver(TestTransformer::new);

    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(SOURCE_TOPIC, new StringSerializer(), new StringSerializer());

    driver.pipeInput(factory.create(SOURCE_TOPIC, "driver", "this is a value for the DRIVER"));
    driver.pipeInput(factory.create(SOURCE_TOPIC, "streams", "kafka streams is EASY"));

    ElasticsearchStateStore store = (ElasticsearchStateStore)driver.getStateStore(ElasticsearchStateStore.STORE_NAME);
    store.flush();

    List<Document> results = store.search("streams", "content.title");

    Assert.assertEquals(1, results.size());
    //assertThat(results.get(0).content, hasKey("title"));
    assertThat(results.get(0).content.get("title").toString(), containsString("streams"));
    driver.close();
  }
}
