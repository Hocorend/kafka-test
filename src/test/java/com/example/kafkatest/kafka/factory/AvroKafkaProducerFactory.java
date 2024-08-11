package com.example.kafkatest.kafka.factory;

import com.example.kafkatestclient.avro.Person;
import com.example.kafkatest.configuration.AppConfig;
import com.example.kafkatest.kafka.serialization.AvroSerializer;
import utility.LoggerFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class AvroKafkaProducerFactory implements Cloneable{

    private KafkaProducer<String, Person> kafkaProducer;

    private static final Logger logger = LoggerFactory.getLogger();

    public AvroKafkaProducerFactory(String bootstrapServers) {
        logger.debug("Creating the kafka producer");
        Properties properties = createKafkaProduceConfig(bootstrapServers);
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    private Properties createKafkaProduceConfig(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.KAFKA_PRODUCER_CLIENT_ID);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    public void sendSync(String topic, String key, Person value){
        try{
            logger.info("sending: {}", value.toString());
            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(topic, key, value)).get();
            Assertions.assertTrue(metadata.hasOffset());
            Assertions.assertTrue(metadata.hasTimestamp());

            logger.debug("Record was sent, key: {} | topic: {} | partition: {} | offset: {}", key, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
        }


    }

    static byte[] serialize(final Object obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            out.flush();
            return bos.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
