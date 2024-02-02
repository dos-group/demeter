package de.tu_berlin.dos.demeter.optimizer.utils;

import de.tu_berlin.dos.demeter.optimizer.structures.Observation;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Evaluate {

    public static <T> Map<T, Double> clustering(Map<T, TimeSeries> candidates, float distance) {

        Map<T, Double> valid = new HashMap<>();
        int largest = 0;
        // compare distance of each data point with every other and cluster them
        for (Entry<T, TimeSeries> curr : candidates.entrySet()) {

            int group = 0;
            Observation currLast = curr.getValue().getLast();
            for (Entry<T, TimeSeries> that : candidates.entrySet()) {

                if (curr.getKey() != that.getKey()) {

                    Observation thatLast = that.getValue().getLast();
                    if (Math.abs((currLast.value - thatLast.value) / thatLast.value) < distance) group++;
                }
            }
            if (largest < group) {

                largest = group;
                valid = new HashMap<>();
                valid.put(curr.getKey(), currLast.value);
            }
            else if (largest == group) valid.put(curr.getKey(), currLast.value);
        }
        return valid;
    }

    public static <T> Map<T, Double> regression(Map<T, TimeSeries> candidates, float distance) {

        Map<T, Double> valid = new HashMap<>();
        for (Entry<T, TimeSeries> curr : candidates.entrySet()) {

            // extract previous latency data points
            TimeSeries input = curr.getValue().subSample(0, curr.getValue().size() - 1);
            Observation first = curr.getValue().getFirst();
            Observation last = curr.getValue().getLast();
            SimpleRegression reg = new SimpleRegression();
            input.forEach(e -> reg.addData((e.timestamp - first.timestamp), e.value));
            double predAvgLat = reg.predict(last.timestamp - first.timestamp);
            if (Math.abs((last.value - predAvgLat) / predAvgLat) < distance) valid.put(curr.getKey(), last.value);
        }
        return valid;
    }
}
