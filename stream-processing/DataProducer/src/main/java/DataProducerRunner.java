import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class DataProducerRunner {

    public static void main(String[] args) throws Exception {
        /*
            Tasks to complete:
            - Write enough tests in the DataProducerTest.java file
            - Instantiate the Kafka Producer by following the API documentation
            - Instantiate the DataProducer using the appropriate trace file and the producer
            - Implement the sendData method as required in DataProducer
            - Call the sendData method to start sending data
        */

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.40.109:9092,172.31.46.14:9092,172.31.43.170:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
//        Path traceFilePath = Paths.get("tracefile");
//
//        // Convert the Path to an absolute path
//        String absolutePath = traceFilePath.toAbsolutePath().toString();
        String traceFile = "tracefile";
        DataProducer dataProducer = new DataProducer(producer,traceFile);

        dataProducer.sendData();
        System.out.println("Streaming done");
    }
}
