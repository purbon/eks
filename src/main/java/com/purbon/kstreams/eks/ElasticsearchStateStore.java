package com.purbon.kstreams.eks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.purbon.kstreams.SerdesFactory;
import java.io.IOException;
import org.apache.http.HeaderElement;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class ElasticsearchStateStore implements StateStore, ElasticsearchWritableStore<String, Document> {

  public static final String INDEX = "words";
  public static String STORE_NAME = "ElasticsearchStateStore";
  private final String hostAddr;

  private ElasticsearchChangeLogger<String,Document> changeLogger = null;

  private RestHighLevelClient client;
  private ProcessorContext context;
  private long updateTimestamp;
  private Document value;
  private String key;
  private Serde<Document> docSerdes;

  private final ObjectMapper mapper = new ObjectMapper();

  public ElasticsearchStateStore(String hostAddr) {
    this.hostAddr = hostAddr;
  }

  @Override
  public Document read(String key) {

    if (key == null) {
      return new Document();
    }
    GetRequest request = new GetRequest(INDEX, INDEX, key);
    Document doc = null;
    try {
      GetResponse response = client.get(request);

      String source = response.getSourceAsString();
      doc = mapper.readValue(source, Document.class);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return doc;
  }

  @Override
  public void write(String key, Document value) {
    this.key = key;
    this.value = value;

    IndexRequest request = new IndexRequest(INDEX, INDEX, key);
    String jsonContent;
    try {
      jsonContent = mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      jsonContent = "";
    }

    request.source(jsonContent, XContentType.JSON);


    try {
      client.index(request);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.updateTimestamp = System.currentTimeMillis();
  }

  @Override
  public String name() {
    return STORE_NAME;
  }

  @Override
  public void init(ProcessorContext processorContext, StateStore stateStore) {

    context = processorContext;

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials("elastic", "changeme"));

    client = new RestHighLevelClient(
        RestClient.builder(
            HttpHost.create(hostAddr))
        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
    );

    docSerdes = SerdesFactory.from(Document.class);

    StateSerdes<String,Document> serdes = new StateSerdes(
        name(),
        Serdes.String(),
        docSerdes);

    changeLogger = new ElasticsearchChangeLogger<>(name(), context, serdes);

    context.register(this, (key, value) -> {
      // here the store restore should happen from the changelog topic.
      String sKey = new String(key);
      Document docValue = docSerdes.deserializer().deserialize(sKey, value);
      write(sKey, docValue);
    });

  }

  @Override
  public void flush() {
    changeLogger.logChange(key, value, updateTimestamp);
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean persistent() {
    return true;
  }

  @Override
  public boolean isOpen() {
    try {
      return client.ping(RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

}
