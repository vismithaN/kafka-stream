import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;


public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
      Task 1:
        In Task 1, you need to read the content in the tracefile we give to you, 
        create two streams, and feed the messages in the tracefile to different 
        streams based on the value of "type" field in the JSON string.

        Please note that you're working on an ec2 instance, but the streams should
        be sent to your samza cluster. Make sure you can consume the topics on the
        master node of your samza cluster before you make a submission.
    */
    public void sendData() {
        try(BufferedReader br = new BufferedReader(new FileReader(traceFileName))) {
            String log;
            while((log = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonElement jsonElement = parser.parse(log);
                JsonObject json = jsonElement.getAsJsonObject();

                String type = json.get("type").getAsString();
                int blockId = json.get("blockId").getAsInt();
                String topic = type.equals("DRIVER_LOCATION") ? "driver-locations" : "events";
                int partition = blockId % 5;

                // Send message to the topic and partition
                producer.send(new ProducerRecord<>(topic, partition, null, log));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }

}
