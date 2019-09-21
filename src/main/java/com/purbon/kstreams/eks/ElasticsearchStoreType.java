package com.purbon.kstreams.eks;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class ElasticsearchStoreType<K,V> implements QueryableStoreType<ElasticsearchReadableStore<K,V>> {

  @Override
  public boolean accepts(StateStore stateStore) {
    return stateStore instanceof ElasticsearchStateStore;
  }

  @Override
  public ElasticsearchReadableStore<K, V> create(StateStoreProvider stateStoreProvider, String storeName) {
    return new ElasticsearchStateStoreWrapper<K,V>(stateStoreProvider, storeName, this);
  }
}
