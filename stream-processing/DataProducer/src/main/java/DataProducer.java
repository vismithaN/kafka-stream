import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.BufferedReader;
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
        try (BufferedReader br = new BufferedReader(new FileReader(traceFileName))) {
            String log;
            while ((log = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonElement jsonElement = parser.parse(log);
                JsonObject json = jsonElement.getAsJsonObject();

                String type = json.get("type").getAsString();
                if(!type.equals("DRIVER_LOCATION")) {
                    String topic = "events";
                    if (type.equals("RIDER_STATUS") || type.equals("RIDER_INTEREST")) {
                        // Send to all 5 partitions
                        for (int partition = 0; partition < 5; partition++) {
                            producer.send(new ProducerRecord<>(topic, partition, null, log));
                        }
                    } else if(type.equals("RIDE_REQUEST")) {
                        // Send to specific partition based on blockId
                        int blockId = json.get("blockId").getAsInt();
                        int partition = blockId % 5;
                        producer.send(new ProducerRecord<>(topic, partition, null, log));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
