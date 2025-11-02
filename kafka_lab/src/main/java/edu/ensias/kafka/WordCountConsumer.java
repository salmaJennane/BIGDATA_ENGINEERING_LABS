package edu.ensias.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WordCountConsumer {
    public static void main(String[] args) throws Exception {
        if(args.length == 0){
            System.out.println("Usage: WordCountConsumer <topic-name>");
            return;
        }
        
        String topicName = args[0];
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "wordcount-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        
        Map<String, Integer> wordCounts = new HashMap<>();
        
        System.out.println("=== Word Count Consumer ===");
        System.out.println("Ecoute du topic: " + topicName);
        System.out.println("En attente de messages...\n");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    String word = record.value().toLowerCase();
                    
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    
                    System.out.println("\n--- Comptage mis a jour ---");
                    System.out.printf("Mot recu: '%s' (offset: %d)\n", word, record.offset());
                    System.out.println("\nFrequence des mots:");
                    
                    wordCounts.entrySet().stream()
                        .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                        .forEach(entry -> 
                            System.out.printf("  %-15s : %d\n", entry.getKey(), entry.getValue())
                        );
                    System.out.println("---------------------------");
                }
            }
        } finally {
            consumer.close();
        }
    }
}