package com.purbon.kstreams.hackathon.store;

import java.util.HashMap;
import java.util.Map;

public class Document<K,V> {


  public String docId;
  public Map<String, String> headers;
  public Map<K,V> content;

  public Document() {
    this("", new HashMap<>());
  }
  public Document(String docId, Map<K,V> content) {
    this.content = content;
    this.headers = new HashMap<>();
    this.docId = docId;
  }
}
