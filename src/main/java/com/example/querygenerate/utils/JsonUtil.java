package com.example.querygenerate.utils;

/**
 * @author QuangNN
 */

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonUtil {
    public static Gson gson = new Gson();

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }
    public static String toJson(Object object) {
        return gson.toJson(object);
    }
}
