import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataProducerTest {
    private MockProducer<String, String> producer;

    @Before
    public void setUp() {
        producer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
    }

    /**
     * This test checks if the messages go to the correct topic and partition as required.
     * Additional test cases can be added by adding more entries to test_trace and verifying here.
     * @throws IOException
     */
    @Test
    public void testProducer() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_trace");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(2,history.size());

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<>("events", 3, null, "{\"blockId\":5648,\"type\":\"ENTERING_BLOCK\"}"),
                new ProducerRecord<>("driver-locations", 4, null, "{\"blockId\":5649,\"type\":\"DRIVER_LOCATION\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }

    @Test
    public void testProducerWithCorrectTopic() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_trace1");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(7,history.size());

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<>("events", 2, null, "{\"blockId\":5647,\"driverId\":7806," +
                        "\"latitude\":40.7901188,\"longitude\":-73.9747985,\"type\":\"ENTERING_BLOCK\",\"status\":\"AVAILABLE\"," +
                        "\"rating\":2.14,\"salary\":11,\"gender\":\"F\"}"),
                new ProducerRecord<>("events", 0, null, "{\"blockId\":1930,\"clientId\":6343," +
                        "\"latitude\":40.731471,\"longitude\":-73.9901805,\"type\":\"RIDE_REQUEST\",\"gender_preference\":\"N\"}"),
                new ProducerRecord<>("events", 3, null, "{\"blockId\":1113,\"driverId\":4843," +
                        "\"latitude\":40.7182511,\"longitude\":-74.0053824,\"type\":\"ENTERING_BLOCK\",\"status\":\"UNAVAILABLE\"," +
                        "\"rating\":3.7,\"salary\":26,\"gender\":\"M\"}"),
                new ProducerRecord<>("events", 1, null, "{\"blockId\":6,\"driverId\":3602," +
                        "\"latitude\":40.7014372,\"longitude\":-74.0119515,\"type\":\"LEAVING_BLOCK\",\"status\":\"UNAVAILABLE\"}"),
                new ProducerRecord<>("events", 4, null, "{\"blockId\":1544,\"driverId\":8429," +
                        "\"latitude\":40.7258816,\"longitude\":-73.9775455,\"type\":\"RIDE_COMPLETE\",\"gender\":\"M\",\"rating\":4.79," +
                        "\"user_rating\":4.0,\"salary\":25}"),
                new ProducerRecord<>("driver-locations", 4, null, "{\"driverId\":131,\"blockId\":3214," +
                        "\"latitude\":40.7519871,\"longitude\":-74.0047584,\"type\":\"DRIVER_LOCATION\"}"),
                new ProducerRecord<>("driver-locations", 4, null, "{\"driverId\":133,\"blockId\":3219," +
                        "\"latitude\":49.75198581,\"longitude\":-89.06577584,\"type\":\"DRIVER_LOCATION\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }
}