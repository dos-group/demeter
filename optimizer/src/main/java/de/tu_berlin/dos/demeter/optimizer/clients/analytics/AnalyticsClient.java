package de.tu_berlin.dos.demeter.optimizer.clients.analytics;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AnalyticsClient {

    public final String baseUrl;
    public final AnalyticsRest service;

    public AnalyticsClient(String baseUrl, Gson gson) {

        this.baseUrl = "http://" + baseUrl + "/";
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.MINUTES)
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(10, TimeUnit.MINUTES)
                .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        this.service = retrofit.create(AnalyticsRest.class);
    }

    public JsonObject rescale(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.rescale(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing rescale: " + response.message());
    }

    public JsonObject profile(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.profile(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing profile: " + response.message());
    }
}
