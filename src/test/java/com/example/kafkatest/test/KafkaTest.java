package com.example.kafkatest.test;

import com.example.kafkatest.configuration.AppConfig;
import com.example.kafkatest.kafka.factory.AvroKafkaConsumerFactory;
import com.example.kafkatest.kafka.factory.AvroKafkaProducerFactory;
import com.example.kafkatest.kafka.factory.KafkaConsumerFactory;
import com.example.kafkatest.kafka.factory.KafkaProducerFactory;
import com.example.kafkatestclient.avro.Person;
import com.example.kafkatest.kafka.serialization.AvroDeserializer;
import utility.FileSaver;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class KafkaTest {

    String topic = "my-topic";
    List<Person> people = new ArrayList<>();
    KafkaProducerFactory producer = new KafkaProducerFactory(AppConfig.BOOTSTRAP_SERVERS);
    KafkaConsumerFactory consumer = new KafkaConsumerFactory(AppConfig.BOOTSTRAP_SERVERS, 10, topic);
    AvroKafkaProducerFactory avroProducer = new AvroKafkaProducerFactory(AppConfig.BOOTSTRAP_SERVERS);
    AvroKafkaConsumerFactory avroConsumer = new AvroKafkaConsumerFactory(AppConfig.BOOTSTRAP_SERVERS, 10, topic);
    File fileOut;

    @Test
    public void kafkaTest(){

        producer.sendSync(topic, AppConfig.KAFKA_PRODUCER_KEY,"Test value 2");

        List<String> results = consumer.consume(10,20);

        for ( String result : results) {
            System.out.println(result);
        }
    }

    @Test
    public void kafkaAvroTest() throws IOException {

        Person person1 = Person.newBuilder()
                .setId("1")
                .setLastname("lastName")
                .setName("TestName").build();

        Person person2 = Person.newBuilder()
                .setId("2")
                .setLastname("lastName2")
                .setName("TestName2").build();

        avroProducer.sendSync(topic, "2", person1);
        avroProducer.sendSync(topic, "4", person2);

        fileOut = new File("src/test/java/com/example/kafkatest/test/data/avroMessage_Person_"+Instant.now().getNano()+".txt");

        List<byte[]> results = avroConsumer.consume(10,20);

        for ( byte[] result : results) {
            String base64 = new String(Base64.getEncoder().encode(result))+"\n";
            System.out.println(base64);
            FileSaver.saveToFile(base64, fileOut);

            people.add(AvroDeserializer.deserializeAvro(result, Person.class).get());
        }
        for (Person person: people) {

            System.out.println(person.toString());
        }
    }
}
