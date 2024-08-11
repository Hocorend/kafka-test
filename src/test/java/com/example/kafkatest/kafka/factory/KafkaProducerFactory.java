package com.example.kafkatest.kafka.factory;

import com.example.kafkatest.configuration.AppConfig;
import utility.LoggerFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class KafkaProducerFactory implements Cloneable{

    private KafkaProducer<String,String> kafkaProducer;

    private static final Logger logger = LoggerFactory.getLogger();

    public KafkaProducerFactory(String bootstrapServers) {
        logger.debug("Creating the kafka producer");
        Properties properties = createKafkaProduceConfig(bootstrapServers);
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    private Properties createKafkaProduceConfig(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.KAFKA_PRODUCER_CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    public void sendSync(String topic, String key, String value){
        try{
            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(topic, key, value)).get();
            logger.debug("Record was sent, key: {} | topic: {} | partition: {} | offset: {}", key, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
        }
    }
}
