package de.tu_berlin.dos.demeter.optimizer.structures;

import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class TimeSeries implements Serializable {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    public static final Logger LOG = LogManager.getLogger(TimeSeries.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static TimeSeries create() {

        return new TimeSeries(new LinkedList<>());
    }

    public static TimeSeries create(long startTs, long endTs) {

        int startTsLen = (int) Math.log10(startTs) + 1;
        int endTsLen = (int) Math.log10(endTs) + 1;
        int valueInc;
        if (startTsLen == 10 && endTsLen == 10) valueInc = 1;
        else if (startTsLen == 13 && endTsLen == 13) valueInc = 1000;
        else throw new IllegalStateException(String.format(
            "Invalid timestamp format: %d (%d) : %d (%d)",
            startTs, startTsLen, endTs, endTsLen));
        LinkedList<Observation> observations = new LinkedList<>();
        for (long i = startTs; i <= endTs; i += valueInc) {

            observations.add(new Observation(i, null));
        }
        return new TimeSeries(observations);
    }

    public static TimeSeries fromCSV(File file, String sep, boolean header) throws Exception {

        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean headerRead = false;
            while ((line = br.readLine()) != null) {
                if (header && !headerRead) {
                    headerRead = true;
                    continue;
                }
                lines.add(line);
            }
        }
        // create time series using first and last element
        TimeSeries ts =
            TimeSeries.create(
                Long.parseLong(lines.get(0).split(sep)[0]),
                Long.parseLong(lines.get(lines.size() - 1).split(sep)[0]));

        for (String line : lines) {

            String[] values = line.split(sep);
            try {
                ts.setObservation(new Observation(Long.parseLong(values[0]), Double.parseDouble(values[1])));
            }
            catch (Exception e) {

                LOG.error(e.getMessage());
            }
        }
        return ts;
    }

    public static void toCSV(String fileName, TimeSeries ts, String header, String sep) throws IOException {

        Path filePath = Paths.get(fileName);
        File file = filePath.toFile();
        boolean doAppend = file.exists();
        if (!doAppend) {

            LOG.info(String.format("File not found, creating %s", fileName));
            Files.createDirectories(filePath.getParent());
            file.createNewFile();
        }
        else LOG.info(String.format("File found, appending to %s", fileName));

        FileWriter fw = new FileWriter(file, doAppend);
        if (!doAppend) fw.write(header + "\n");
        for (Observation observation : ts.observations) {

            fw.write(observation.timestamp + sep + observation.value + "\n");
        }
        fw.close();
    }

    public static TimeSeries merge(TimeSeries ts1, TimeSeries ts2) {

        long first, last;
        if (ts1.getFirst().timestamp < ts2.getFirst().timestamp) first = ts1.getFirst().timestamp;
        else first = ts2.getFirst().timestamp;
        if (ts1.getLast().timestamp > ts2.getLast().timestamp) last = ts1.getLast().timestamp;
        else last = ts2.getLast().timestamp;

        TimeSeries merged = TimeSeries.create(first, last);
        for (Observation curr : merged.observations) {

            int ob1Idx = ts1.getIndex(curr.timestamp);
            int ob2Idx = ts2.getIndex(curr.timestamp);

            if (ob1Idx != -1 && ob2Idx == -1) merged.setObservation(ts1.observations.get(ob1Idx));
            else if (ob1Idx == -1 && ob2Idx != -1) merged.setObservation(ts2.observations.get(ob2Idx));
            else if (ob1Idx != -1 && ob2Idx != -1) {

                Double sum = null;
                if (ts1.observations.get(ob1Idx).value != null && ts2.observations.get(ob2Idx).value == null)
                    sum = ts1.observations.get(ob1Idx).value;
                else if (ts1.observations.get(ob1Idx).value == null && ts2.observations.get(ob2Idx).value != null)
                    sum = ts2.observations.get(ob2Idx).value;
                else if (ts1.observations.get(ob1Idx).value != null && ts2.observations.get(ob2Idx).value != null)
                    sum = ts1.observations.get(ob1Idx).value + ts2.observations.get(ob2Idx).value;
                merged.setObservation(new Observation(curr.timestamp, sum));
            }
        }
        return merged;
    }

    public static TimeSeries asyncMerge(ExecutorService executor, List<TimeSeries> tsList) throws Exception {

        TimeSeries merged;
        if (tsList.size() == 2) merged = merge(tsList.get(0), tsList.get(1));
        else if (tsList.size() == 3) {

            merged = merge(tsList.get(0), tsList.get(1));
            merged = merge(merged, tsList.get(2));
        }
        else {

            int partSize = IntMath.divide(tsList.size(), 2, RoundingMode.UP);
            List<List<TimeSeries>> parts = Lists.partition(tsList, partSize);
            Future<TimeSeries> futureA = executor.submit(() -> asyncMerge(executor, parts.get(0)));
            Future<TimeSeries> futureB = executor.submit(() -> asyncMerge(executor, parts.get(1)));
            merged = merge(futureA.get(), futureB.get());
        }
        return merged;
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private LinkedList<Observation> observations;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private TimeSeries(LinkedList<Observation> observations) {

        Collections.sort(observations);
        this.observations = observations;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public int size() {

        return this.observations.size();
    }

    public TimeSeries resample(int sampleRate, Integer limit) {

        long last = this.observations.get(this.observations.size() - 1).timestamp;
        return resample(last, sampleRate, limit);
    }

    public TimeSeries resample(long timestamp, int sampleRate, Integer limit) {

        // find timestamp in observations
        int i = this.getIndex(timestamp);

        LinkedList<Observation> result = new LinkedList<>();
        if (observations.get(i).timestamp == timestamp) {
            // calculate the number of samples in new time series
            int count = (limit != null) ? limit : i + 1;
            // iterate backwards of linked list from matching timestamp
            int j = 0;
            ListIterator<Observation> iterator = observations.listIterator(i + 1);
            while(iterator.hasPrevious() && result.size() < count) {
                // retrieve sample
                Observation obs = iterator.previous();
                // test if current sample index is within sample rate
                if (j % sampleRate == 0) {
                    // append valid sample to front of results
                    result.addFirst(obs);
                }
                ++j;
            }
        }
        return new TimeSeries(result);
    }

    public TimeSeries subSample(int startIndexInc, int endIndexExc) {

        TimeSeries ts = TimeSeries.create();
        for (int i = startIndexInc; i < endIndexExc; i++) {

            ts.getObservations().add(this.observations.get(i));
        }
        return ts;
    }

    public int getIndex(long timestamp) {

        return this.observations.indexOf(new Observation(timestamp, null));
    }

    public LinkedList<Observation> getObservations() {

        return this.observations;
    }

    public void setObservation(Observation observation) {

        int index = this.observations.indexOf(observation);
        if (index != -1) this.observations.set(index, observation);
        else throw new IllegalStateException(String.format("Unknown observation timestamp: %s", observation.timestamp));
    }

    public Observation getObservation(long timestamp) {

        int index = this.getIndex(timestamp);
        if (index == -1) return null;
        return observations.get(index);
    }

    public double[] values() {

        return this.observations.stream().mapToDouble(v -> v.value).toArray();
    }

    public Observation getFirst() {

        return this.observations.getFirst();
    }

    public Observation getLast() {

        return this.observations.getLast();
    }

    public double avg() {

        double sum = 0;
        int count = 0;
        for (Observation observation : this.observations) {

            if (observation.value != null) {

                sum += observation.value;
                count++;
            }
        }
        return sum / count;
    }

    public double min() {

        double min = 0;
        for (Observation observation : this.observations) {

            if (observation.value != null && observation.value < min) {

                min = observation.value;
            }
        }
        return min;
    }

    public double max() {

        double max = 0;
        for (Observation observation : this.observations) {

            if (observation.value != null && max < observation.value) {

                max = observation.value;
            }
        }
        return max;
    }

    public double sum() {

        double total = 0;
        for (Observation observation : this.observations) {

            Double value = observation.value;
            if (value != null) total += value;
        }
        return total;
    }

    public void forEach(Consumer<Observation> behaviour) {

        this.observations.forEach(behaviour);
    }

    @Override
    public String toString() {

        return "{\"observations\":" + observations + ",\"count\":" + observations.size() + '}';
    }
}
