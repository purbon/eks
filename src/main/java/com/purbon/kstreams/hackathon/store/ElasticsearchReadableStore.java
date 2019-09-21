package com.purbon.kstreams.hackathon.store;

public interface ElasticsearchReadableStore<K,V>  {

  V read(K key);

}
