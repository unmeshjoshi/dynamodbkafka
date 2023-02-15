package dynamodbkafka;

public class VehicleMessage {
    private int vin;
    private int tyrePressure;

    public VehicleMessage(int vin, int tyrePressure) {
        this.vin = vin;
        this.tyrePressure = tyrePressure;
    }

    public int getVin() {
        return vin;
    }

    public int getTyrePressure() {
        return tyrePressure;
    }
}
