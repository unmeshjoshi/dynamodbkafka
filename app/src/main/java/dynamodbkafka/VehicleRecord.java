package dynamodbkafka;

import com.amazonaws.services.dynamodbv2.datamodeling.*;

import java.util.Optional;

@DynamoDBTable(tableName="VehicleRecord")
public class VehicleRecord {

    private Integer vin;
    private Integer tyrePressure;
    private Long version;
    private long kafkaOffset;

    boolean offsetAlreadyProcessed(long offset) {
        return Optional.of(getKafkaOffset()).stream().allMatch(o -> o > offset);
    }

    @DynamoDBHashKey(attributeName="VIN")
    public Integer getVin() { return vin; }
    public void setVin(Integer Id) { this.vin = Id; }

    @DynamoDBAttribute(attributeName="TirePressure")
    public Integer getTyrePressure() { return tyrePressure; }
    public void setTyrePressure(Integer tyrePressure) { this.tyrePressure = tyrePressure; }

    @DynamoDBVersionAttribute(attributeName="Version")
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version;}

    @DynamoDBAttribute(attributeName="KafkaOffset")
    public Long getKafkaOffset() {
        return kafkaOffset;
    }
    public void setKafkaOffset(Long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }
}