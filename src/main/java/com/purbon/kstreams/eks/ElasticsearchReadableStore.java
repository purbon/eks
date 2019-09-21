package com.purbon.kstreams.eks;

import java.util.List;

public interface ElasticsearchReadableStore<K,V>  {

  V read(K key);

  List<V> search(String words, String... fields);

}
