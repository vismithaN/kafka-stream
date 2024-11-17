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
    public void testProducerWithEventsTopic() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_trace2");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(11,history.size());

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<>("events", 0, null, "{\"blockId\":45,\"clientId\":0," +
                        "\"latitude\":40.6799404643263,\"longitude\":-73.980282552649,\"type\":\"RIDE_REQUEST\"," +
                        "\"gender_preference\":\"N\"}"),

                new ProducerRecord<>("events", 0, null, "{\"userId\":1,\"interest\":\"ramen\"," +
                        "\"duration\":1565434,\"type\":\"RIDER_INTEREST\"}"),

                new ProducerRecord<>("events", 1, null, "{\"userId\":1,\"interest\":\"ramen\"," +
                        "\"duration\":1565434,\"type\":\"RIDER_INTEREST\"}"),

                new ProducerRecord<>("events", 2, null, "{\"userId\":1,\"interest\":\"ramen\"," +
                        "\"duration\":1565434,\"type\":\"RIDER_INTEREST\"}"),

                new ProducerRecord<>("events", 3, null, "{\"userId\":1,\"interest\":\"ramen\"," +
                        "\"duration\":1565434,\"type\":\"RIDER_INTEREST\"}"),

                new ProducerRecord<>("events", 4, null, "{\"userId\":1,\"interest\":\"ramen\"," +
                        "\"duration\":1565434,\"type\":\"RIDER_INTEREST\"}"),

                new ProducerRecord<>("events", 0, null, "{\"userId\":3,\"mood\":5,\"blood_sugar\":4," +
                        "\"stress\":7,\"active\":0,\"type\":\"RIDER_STATUS\"}"),

                new ProducerRecord<>("events", 1, null, "{\"userId\":3,\"mood\":5,\"blood_sugar\":4,\"stress\":7," +
                        "\"active\":0,\"type\":\"RIDER_STATUS\"}"),

                new ProducerRecord<>("events", 2, null, "{\"userId\":3,\"mood\":5,\"blood_sugar\":4," +
                        "\"stress\":7,\"active\":0,\"type\":\"RIDER_STATUS\"}"),

                new ProducerRecord<>("events", 3, null, "{\"userId\":3,\"mood\":5,\"blood_sugar\":4," +
                        "\"stress\":7,\"active\":0,\"type\":\"RIDER_STATUS\"}"),

                new ProducerRecord<>("events", 4, null, "{\"userId\":3,\"mood\":5,\"blood_sugar\":4," +
                        "\"stress\":7,\"active\":0,\"type\":\"RIDER_STATUS\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }
}