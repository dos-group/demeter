package de.tu_berlin.dos.demeter.optimizer.clients.prometheus;

import com.google.gson.Gson;
import de.tu_berlin.dos.demeter.optimizer.clients.prometheus.PrometheusClient.Matrix.MatrixData.MatrixResult;
import de.tu_berlin.dos.demeter.optimizer.structures.Observation;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import okhttp3.OkHttpClient;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PrometheusClient {

    public static class Matrix {

        public static class MatrixData {

            public static class MatrixResult {

                public Map<String, String> metric;
                public List<List<String>> values;

                @Override
                public String toString() {

                    return String.format("{metric:%s,values:%s}", metric.toString(), values == null ? "" : values.toString());
                }
            }

            public String resultType;
            public List<MatrixResult> result;

            @Override
            public String toString() {

                return "{resultType:'" + resultType + "\',result:" + result + '}';
            }
        }

        public String status;
        public MatrixData data;

        @Override
        public String toString() {

            return "{status:'" + status + "\',data:" + data + '}';
        }
    }

    private static final int LIMIT = 11000;

    private final PrometheusRest service;

    public PrometheusClient(String baseUrl, Gson gson) {

        OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.MINUTES)
            .readTimeout(100, TimeUnit.MINUTES)
            .writeTimeout(100, TimeUnit.MINUTES)
            //.addInterceptor(logging)
            .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl("http://" + baseUrl + "/")
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        this.service = retrofit.create(PrometheusRest.class);
    }

    public Map<String, TimeSeries> queryRange(String query, String metric, long start, long end) throws IOException {

        Map<String, TimeSeries> data = new HashMap<>();

        long current = start;
        long iterations = (end - start) / LIMIT;
        for (int i = 0; i <= iterations; i++) {

            long last = current + (end - start) % LIMIT;
            if (i < iterations) last = current + LIMIT;

            Response<Matrix> response = this.service.queryRange(query, current, last, 1, 300).execute();
            if (!response.isSuccessful()) String.format("Prometheus error: %s", response.errorBody().string());
            else {

                Matrix matrix = response.body();
                for (MatrixResult result : matrix.data.result) {

                    String key = result.metric.get(metric);
                    if (key.isEmpty()) throw new IllegalStateException("Prometheus error, metric not found: " + metric);
                    TimeSeries value = data.get(key);
                    if (value == null) {

                        value = TimeSeries.create(start, end);
                        data.put(key, value);
                    }
                    for (List<String> strings : result.values) {

                        Long ts = Long.parseLong(strings.get(0));
                        Double val = Double.parseDouble(strings.get(1));
                        if (Double.isNaN(val)) val = null;
                        value.setObservation(new Observation(ts, val));
                    }
                }
            }
            current += LIMIT;
        }
        return data;
    }

    public TimeSeries queryRange(String query, long start, long end, int step) throws IOException {

        TimeSeries ts = TimeSeries.create(start, end);

        long current = start;
        long iterations = (end - start) / LIMIT;
        for (int i = 0; i <= iterations; i++) {

            long last = current + (end - start) % LIMIT;
            if (i < iterations) last = current + LIMIT;

            Matrix matrix = this.service.queryRange(query, current, last, step, 300).execute().body();
            if (matrix != null && matrix.status.equalsIgnoreCase("success") && matrix.data.result.size() > 0) {

                for (List<String> strings : matrix.data.result.get(0).values) {

                    Long timestamp = Long.parseLong(strings.get(0));
                    Double value = Double.parseDouble(strings.get(1));
                    if (Double.isNaN(value)) value = null;
                    ts.setObservation(new Observation(timestamp, value));
                }
            }
            current += LIMIT;
        }
        return ts;
    }

    public TimeSeries queryRange(String query, long start, long end) throws IOException {

       return this.queryRange(query, start, end, 1);
    }
}
