package de.tu_berlin.dos.demeter.optimizer.structures;

import java.io.Serializable;
import java.util.Objects;

public class Observation implements Serializable, Comparable<Observation> {

    public final Long timestamp;
    public final Double value;

    public Observation(Long timestamp, Double value) {

        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(Observation that) {

        return Long.compare(this.timestamp, that.timestamp);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (!(o instanceof Observation that)) return false;
        return Objects.equals(this.timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(timestamp);
    }

    @Override
    public String toString() {

        return "{\"timestamp\":" + timestamp + ",\"value\":" + value + '}';
    }
}
