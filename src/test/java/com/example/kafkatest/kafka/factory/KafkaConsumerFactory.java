package com.example.kafkatest.kafka.factory;

import com.example.kafkatest.configuration.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;

import org.awaitility.core.ConditionTimeoutException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerFactory {


    private String topic;

    private Properties properties;

    private static final boolean AUTO_COMMIT = false;

    private static final String AUTO_OFFSET_RESET = "earliest";

    public KafkaConsumerFactory(String bootstrapServers, int max_poll_records, String topic){
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
        return properties;
    }

    public List<String> consume(int expectedRecordsCount, int timeAllowed){
        List<String> results = new ArrayList<>();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())){
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

        return results;
    }

    private List<String> poolRecords(KafkaConsumer<String, String> kafkaConsumer, List<String> results) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));

        if (consumerRecords.count() != 0 ) consumerRecords.forEach((record)->{
            String recordOneLine = record.value().toString().replaceAll("(\r\n)","\\\\r\\\\n");

            results.add(record.value());
            kafkaConsumer.commitSync();
        });

        return results;
    }


}
