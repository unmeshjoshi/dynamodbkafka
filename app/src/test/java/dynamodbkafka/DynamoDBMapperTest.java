package dynamodbkafka;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import dynamodbkafka.utils.LocalDynamoDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DynamoDBMapperTest {
    private LocalDynamoDB localDynamoDB = new LocalDynamoDB();

    @Before
    public void setUp() throws Exception {
        setupDynamoDB();
    }

    @After
    public void shutdown() {
        localDynamoDB.stop();
    }

    private void setupDynamoDB() throws Exception {
        localDynamoDB.start();
        localDynamoDB.createTables(Arrays.asList(VehicleRecord.class));
    }

    @Test
    public void loadStoreBasicRecord() {
        DynamoDBMapper mapper = new DynamoDBMapper(localDynamoDB.getClient());

        int vin = 1;
        VehicleRecord vehicleRecord = mapper.load(VehicleRecord.class, vin);
        if (vehicleRecord == null) {
            vehicleRecord = new VehicleRecord();
        }
        //update vehicle record
        vehicleRecord.setVin(vin);
        vehicleRecord.setTyrePressure(100);
        mapper.save(vehicleRecord);

        VehicleRecord loadedRecord = mapper.load(VehicleRecord.class, vin);
        assertEquals(loadedRecord.getVersion(), Long.valueOf(1));
        assertEquals(loadedRecord.getVin(), Integer.valueOf(1));
        assertEquals(loadedRecord.getTyrePressure(), Integer.valueOf(100));
    }

}
