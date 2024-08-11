package com.example.kafkatest.kafka.serialization;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

public class AvroDeserializer {

    private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();

    public static <T extends SpecificRecord>Optional <T> deserializeAvro(byte[]binaryData, Class<T> clazz){
        if(binaryData == null || binaryData.length == 0){
            return Optional.empty();
        }

        SpecificDatumReader<T> specificDatumReader = new SpecificDatumReader<>(clazz);
        BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(new ByteArrayInputStream(binaryData), null);

        try {
            return Optional.of(specificDatumReader.read(null, binaryDecoder));
        } catch (IOException e){
            throw new RuntimeException(e.getMessage());
        }
    }
}
