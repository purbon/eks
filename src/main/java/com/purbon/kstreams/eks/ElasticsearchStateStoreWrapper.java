package com.purbon.kstreams.eks;

import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class ElasticsearchStateStoreWrapper<K,V> implements ElasticsearchReadableStore<K,V> {


  private final StateStoreProvider provider;
  private final String storeName;
  private final QueryableStoreType<ElasticsearchReadableStore<K, V>> elasticsearchStoreType;

  public ElasticsearchStateStoreWrapper(final StateStoreProvider provider,
      final String storeName,
      final QueryableStoreType<ElasticsearchReadableStore<K,V>> elasticsearchStoreType) {

    this.provider = provider;
    this.storeName = storeName;
    this.elasticsearchStoreType = elasticsearchStoreType;
  }
  @Override
  public V read(K key) {

    List<ElasticsearchReadableStore<K,V>> stores = provider.stores(storeName, elasticsearchStoreType);
    Optional<ElasticsearchReadableStore<K,V>> value = stores
        .stream()
        .filter(store -> store.read(key) != null)
        .findFirst();

    return value.map(store -> store.read(key)).orElse(null);
  }
}
