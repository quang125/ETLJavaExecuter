package com.example.querygenerate;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Dim {
    private String tableName;
    private List<String> keyColumn;
    private String id;
    private String tableConfigName;
}


