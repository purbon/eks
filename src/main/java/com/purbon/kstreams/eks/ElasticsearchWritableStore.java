package com.purbon.kstreams.eks;

public interface ElasticsearchWritableStore<K,V> extends ElasticsearchReadableStore<K,V> {

  void write(K key, V value);

}
