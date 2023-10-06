package com.example.querygenerate.data;

import com.google.gson.Gson;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author QuangNN
 */
@Data
public class Fact{
    private String tableName;
    private int timeStep;
    private int backupType;
    private int timeVacuum;
    private String checkColumn;
    private int etlThreadNumber;
    private String etlQueryCommand;
    private String etlQueryCheckCommand;
    private String etlMap;
    private String rawTable;
    private String id;
    private String tableConfigName;
    private String createdDateStr;

    public EtlMap getEtlMap(){
        return new Gson().fromJson(this.etlMap,EtlMap.class);
    }
}
