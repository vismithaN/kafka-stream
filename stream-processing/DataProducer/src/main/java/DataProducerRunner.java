import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.32.66:9092,172.31.44.79:9092,172.31.32.106:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        String traceFile = "trace_bonus";
        DataProducer dataProducer = new DataProducer(producer,traceFile);

        dataProducer.sendData();
        producer.close();
        System.out.println("Streaming done");
    }
}
