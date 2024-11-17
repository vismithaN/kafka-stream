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
    public void testProducerWithAdClickTopic() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_bonus");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(2,history.size());

        List<ProducerRecord<String, String>> expected = Arrays.asList(

                new ProducerRecord<>("ad-click", 1, null, "{\"userId\": 19036, " +
                        "\"storeId\": \"44SY464xDHbvOcjDzRbKkQ\", \"name\": \"Ippudo NY\", \"clicked\": \"false\"}"),

                new ProducerRecord<>("ad-click", 2, null, "{\"userId\": 13617, " +
                        "\"storeId\": \"44SY464xDHbvOcjDzRbKkQ\", \"name\": \"Ippudo NY\", \"clicked\": \"true\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }
}