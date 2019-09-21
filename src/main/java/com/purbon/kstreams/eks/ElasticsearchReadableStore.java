package com.purbon.kstreams.eks;

public interface ElasticsearchReadableStore<K,V>  {

  V read(K key);

}
