package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am callback producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer  = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            // create the producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world"+i);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record successfully sent or exception is thrown
                    if(e == null){
                        // the record was successfully sent
                        String rs = "Receive new metadata \n" +
                                "Topic: "+ recordMetadata.topic()+
                                "\nPartition: "+ recordMetadata.partition()+
                                "\nOffset: "+ recordMetadata.offset()+
                                "\nTimestamp: "+ recordMetadata.timestamp();
                        log.info(rs);
                    }
                }
            });
        }

        // tell the producer to send all data and block util done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
