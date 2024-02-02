package de.tu_berlin.dos.demeter.optimizer.clients.generator;

import com.google.gson.JsonObject;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;

public interface GeneratorRest {

    @Headers("Accept: application/json")
    @GET("generators/status")
    Call<JsonObject> status();

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("generators/start")
    Call<JsonObject> start(
            @Body JsonObject body
    );

    @Headers({
        "Accept: application/json",
    })
    @POST("generators/stop")
    Call<JsonObject> stop();

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("generators/workload")
    Call<JsonObject> workload(
            @Body JsonObject body
    );
}
