package dynamodbkafka;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonSerDesTest {

    @Test
    public void serDesTest() {
        VehicleMessage message = new VehicleMessage(1, 1);
        assertEquals(JsonSerDes.deserialize(JsonSerDes.serialize(message), VehicleMessage.class), message);
    }
}
