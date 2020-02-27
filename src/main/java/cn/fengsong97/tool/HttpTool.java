package cn.fengsong97.tool;

import okhttp3.*;

import java.io.IOException;

public  class HttpTool {
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static OkHttpClient client;

    public static synchronized OkHttpClient getInstance() {
        if (client == null) {
            client = new OkHttpClient();
        }
        return client;
    }

    public static Response postJson(String url,String json) throws IOException{
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
       Response response = getInstance().newCall(request).execute();
            return response;

    }

    public static Response getJson(String url,String json) throws IOException{
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(url)
                .build();
        Response response = getInstance().newCall(request).execute();
        return response;

    }
}
