package dynamodbkafka;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VehicleMessage that = (VehicleMessage) o;
        return vin == that.vin && tyrePressure == that.tyrePressure;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vin, tyrePressure);
    }

    @Override
    public String toString() {
        return "VehicleMessage{" +
                "vin=" + vin +
                ", tyrePressure=" + tyrePressure +
                '}';
    }
}
