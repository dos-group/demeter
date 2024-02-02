package de.tu_berlin.dos.demeter.optimizer.clients.analytics;

import com.google.gson.JsonObject;
import retrofit2.Call;
import retrofit2.http.*;

public interface AnalyticsRest {

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("optimization/rescale")
    Call<JsonObject> rescale(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("optimization/profile")
    Call<JsonObject> profile(
            @Body JsonObject body
    );
}
