package de.tu_berlin.dos.demeter.optimizer.clients.flink;

import com.google.gson.JsonObject;
import okhttp3.MultipartBody;
import retrofit2.Call;
import retrofit2.http.*;

public interface FlinkRest {

    @Multipart
    @POST("v1/jars/upload")
    Call<JsonObject> uploadJar(
        @Part MultipartBody.Part jarfile
    );

    @Headers({
        "Accept: application/json",
    })
    @GET("v1/jobs")
    Call<JsonObject> getJobs(

    );

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("v1/jars/{jarId}/run")
    Call<JsonObject> startJob(
        @Path("jarId") String jarId,
        @Body JsonObject body
    );

    @PATCH("v1/jobs/{jobId}")
    Call<Void> stopJob(
        @Path("jobId") String jobId
    );

    @POST("v1/jobs/{jobId}/savepoints")
    Call<JsonObject> saveJob(
        @Path("jobId") String jobId,
        @Body JsonObject body
    );

    @GET("v1/jobs/{jobId}/savepoints/{requestId}")
    Call<JsonObject> checkStatus(
        @Path("jobId") String jobId,
        @Path("requestId") String requestId
    );

    @Headers({
        "Accept: application/json",
    })
    @GET("v1/jobs/{jobId}/plan/")
    Call<JsonObject> getVertices(
        @Path("jobId") String jobId
    );

    @Headers({
        "Accept: application/json",
    })
    @GET("v1/jobs/{jobId}/vertices/{vertexId}/taskmanagers")
    Call<JsonObject> getTaskManagers(
        @Path("jobId") String jobId,
        @Path("vertexId") String vertexId
    );

    @Headers({
        "Accept: application/json",
    })
    @GET("v1/jobs/{jobId}/")
    Call<JsonObject> getLatestTs(
        @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}/checkpoints/")
    Call<JsonObject> getChkInfo(
        @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}")
    Call<JsonObject> getJob(
        @Path("jobId") String jobId
    );

}
