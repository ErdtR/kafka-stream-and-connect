import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class Main {
    public static void main(String[] args){
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<GenericRecord, GenericRecord> messages =
                streamBuilder.stream("avro-topic");


        // Simple stream
        KTable<String, Long> countUsers = messages
            .mapValues(value -> value.get("user").toString())
                .selectKey((key, value) -> value)
                .groupByKey(Serdes.String(), Serdes.String())
                .count();


        countUsers.toStream().print(Serdes.String(), Serdes.Long());
        //countUsers.toStream().to(Serdes.String(), Serdes.Long(),"count-topic");

        KafkaStreams streams = new KafkaStreams(streamBuilder, properties);
        streams.start();

    }
}
