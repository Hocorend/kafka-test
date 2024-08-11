package com.example.kafkatest.kafka.factory;

import com.example.kafkatest.configuration.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AvroKafkaConsumerFactory {

    private String topic;

    private Properties properties;

    private static final boolean AUTO_COMMIT = false;

    private static final String AUTO_OFFSET_RESET = "earliest";

    public AvroKafkaConsumerFactory(String bootstrapServers, int max_poll_records, String topic){
        this.topic = topic;
        this.properties = createKafkaConsumerConfig(bootstrapServers, max_poll_records);
    }

    private static Properties createKafkaConsumerConfig(String bootstrapServers, int max_poll_records) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.KAFKA_CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max_poll_records);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  ByteArrayDeserializer.class);
        return properties;
    }

    public List<byte[]> consume(int expectedRecordsCount, int timeAllowed){
        List<byte[]> results = new ArrayList<>();

        try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties)){
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            int finalRecordsExpected = poolRecords(kafkaConsumer, results).size() + expectedRecordsCount;

            Awaitility.await().atMost(timeAllowed, TimeUnit.SECONDS)
                    .pollInSameThread()
                    .with()
                    .pollInterval(500, TimeUnit.MICROSECONDS)
                    .until(()-> poolRecords(kafkaConsumer, results).size() >= finalRecordsExpected);

        } catch (ConditionTimeoutException e){
            if (results.isEmpty()){
                throw new NoSuchElementException(String.format("Нет сообщений"));
            }
        }

//        System.out.println(DatatypeConverter.printBase64Binary(results.get(0).getBytes()));
        return results;
    }

    private List<byte[]> poolRecords(KafkaConsumer<String, byte[]> kafkaConsumer, List<byte[]> results) {
        ConsumerRecords<String, byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));

        if (consumerRecords.count() != 0 ) consumerRecords.forEach((record)->{

            results.add(record.value());
            System.out.println("key " + record.key());
            kafkaConsumer.commitSync();
        });

        return results;
    }


}
