/**
 * Created by colin on 6/19/17.
 */
package com.clark.kafkaproducer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class Main {

    public static void main(String[] args) {

        // simple approach
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("Without Callback");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            System.out.println("Sending message #" + i);
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();

        //  with callback
        System.out.println("With Callback");
        Producer<String, String> anotherProducer = new KafkaProducer<String, String>(props);
        for (int i = 101; i < 200; i++) {
            ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i));
            anotherProducer.send(myRecord, new ProducerCallBack());
        }
        anotherProducer.close();
    }

    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println(metadata.topic()+metadata.offset()+metadata.partition());
        }
    }
}
