package edu.ensias.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WordProducer {
    public static void main(String[] args) throws Exception {
        if(args.length == 0){
            System.out.println("Usage: WordProducer <topic-name>");
            return;
        }
        
        String topicName = args[0];
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== Word Producer ===");
        System.out.println("Tapez des mots (un par ligne) et appuyez sur Entree.");
        System.out.println("Tapez 'exit' pour quitter.");
        System.out.println("Topic: " + topicName);
        System.out.println("=====================\n");
        
        try {
            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine();
                
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
                
                if (!line.trim().isEmpty()) {
                    String[] words = line.trim().split("\\s+");
                    for (String word : words) {
                        if (!word.isEmpty()) {
                            producer.send(new ProducerRecord<>(topicName, word, word));
                            System.out.println("  -> Envoye: " + word);
                        }
                    }
                }
            }
        } finally {
            System.out.println("\nFermeture du producteur...");
            scanner.close();
            producer.close();
        }
    }
}