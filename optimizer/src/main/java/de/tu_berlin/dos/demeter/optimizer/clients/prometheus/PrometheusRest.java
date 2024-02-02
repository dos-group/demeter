package de.tu_berlin.dos.demeter.optimizer.clients.prometheus;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.demeter.optimizer.clients.prometheus.PrometheusClient.Matrix;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface PrometheusRest {

    @GET("api/v1/query_range")
    Call<Matrix> queryRange(
        @Query("query") String query,
        @Query("start") long start,
        @Query("end") long end,
        @Query("step") int step,
        @Query("timeout") int timeout
    );

    @GET("api/v1/query_range")
    Call<JsonObject> queryRangeV2(
            @Query("query") String query,
            @Query("start") long start,
            @Query("end") long end,
            @Query("step") int step,
            @Query("timeout") int timeout
    );
}
