package com.example.querygenerate.data;

import com.google.gson.Gson;
import lombok.Data;

import java.util.List;

@Data
public class Dim {
    private String tableName;
    private String keyColumn;
    private String id;
    private String tableConfigName;

    public List<String> getColumnList() {
        if (this.keyColumn == null) return null;
        return new Gson().fromJson(this.keyColumn, List.class);
    }
}


