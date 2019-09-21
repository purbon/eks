package com.purbon.kstreams.hackathon.store;

import java.util.Map;
import org.apache.kafka.streams.state.StoreBuilder;

public class ElasticsearchStoreBuilder implements StoreBuilder<ElasticsearchStateStore> {

  private final String hostAddr;
  private Map<String, String> config;

  public ElasticsearchStoreBuilder() {
    this("http://localhost:9200");
  }

  public ElasticsearchStoreBuilder(String hostAddr) {
    this.hostAddr = hostAddr;
  }

  @Override
  public StoreBuilder<ElasticsearchStateStore>  withCachingEnabled() {
    return this;
  }

  @Override
  public StoreBuilder<ElasticsearchStateStore>  withCachingDisabled() {
    return this;
  }

  @Override
  public StoreBuilder<ElasticsearchStateStore> withLoggingEnabled(Map<String, String> config) {
    this.config = config;
    return this;
  }

  @Override
  public StoreBuilder<ElasticsearchStateStore>  withLoggingDisabled() {
    return this;
  }

  @Override
  public ElasticsearchStateStore build() {
    return new ElasticsearchStateStore(hostAddr);
  }

  @Override
  public Map<String, String> logConfig() {
    return config;
  }

  @Override
  public boolean loggingEnabled() {
    return false;
  }

  @Override
  public String name() {
    return ElasticsearchStateStore.STORE_NAME;
  }
}
