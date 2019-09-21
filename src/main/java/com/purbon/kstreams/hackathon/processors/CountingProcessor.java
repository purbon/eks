package com.purbon.kstreams.hackathon.processors;

import com.purbon.kstreams.hackathon.store.Document;
import com.purbon.kstreams.hackathon.store.ElasticsearchStateStore;
import java.util.HashMap;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CountingProcessor implements Processor<String, String> {

  private ProcessorContext context;
  private HashMap<String, Long> wordCount;
  private ElasticsearchStateStore store;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    wordCount = new HashMap<>();

    store = (ElasticsearchStateStore) context.getStateStore(ElasticsearchStateStore.STORE_NAME);

  }

  @Override
  public void process(String key, String value) {

    //Stream<String> stream = Arrays.stream(value.split("\\s+"));

    String[] values = value.split("\\s+");
    for(int i=0; i < values.length; i++) {
      String word = values[i];
      if (wordCount.get(word) == null) {
        wordCount.put(word, 0L);
      }
      System.out.println(word+" "+wordCount.size());
      wordCount.put(word, wordCount.get(word)+1);
    };
    Document<String, Long> doc = new Document<>(key, wordCount);
    System.out.println(store != null);
    store.write("acc", doc);
  }

  @Override
  public void close() {

  }
}
