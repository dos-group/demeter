package de.tu_berlin.dos.demeter.optimizer.clients.generator;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.concurrent.TimeUnit;

public class GeneratorClient {

    public final String baseUrl;
    public final GeneratorRest service;
    HttpLoggingInterceptor logging = new HttpLoggingInterceptor();

    public GeneratorClient(String baseUrl, Gson gson) {

        //logging.setLevel(Level.BODY);
        this.baseUrl = "http://" + baseUrl + "/";
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.MINUTES)
            .readTimeout(100, TimeUnit.MINUTES)
            .writeTimeout(100, TimeUnit.MINUTES)
            //.addInterceptor(logging)
            .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();

        this.service = retrofit.create(GeneratorRest.class);
    }

    public JsonObject status() throws Exception {

        Response<JsonObject> response = this.service.status().execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(
            String.format("Generators error: %s", response.errorBody().string()));
    }

    public JsonObject start(JsonObject body) throws Exception {

        Response<JsonObject> response = this.service.start(body).execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(
            String.format("Generator error: %s", response.errorBody().string()));
    }

    public JsonObject stop() throws Exception {

        Response<JsonObject> response = this.service.stop().execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(
            String.format("Generator error: %s", response.errorBody().string()));
    }

    public JsonObject workload(JsonObject body) throws Exception {

        Response<JsonObject> response = this.service.workload(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException(
            String.format("Generator error: %s", response.errorBody().string()));
    }

}
