package com.purbon.kstreams.hackathon.store;

public interface ElasticsearchWritableStore<K,V> extends ElasticsearchReadableStore<K,V> {

  void write(K key, V value);

}
