package com.example.querygenerate.converter;

import com.example.querygenerate.data.json.TaskJson;
import com.example.querygenerate.solvers.task.FileStringTaskSolver;
import com.example.querygenerate.utils.JsonUtil;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * @author QuangNN
 */
@RequiredArgsConstructor
@Component
public class JsonConverter {
    private static final List<String> factTables = Arrays.asList("fact_level_daily_time_play", "fact_inapp", "fact_property", "fact_level_difficulty",
            "fact_account_ads_view", "fact_session_time", "fact_resource_log", "fact_retention", "fact_ads_view", "fact_action", "fact_funnel");
    private static final List<String> schema=Arrays.asList("dwh_falcon_1", "dwh_falcon_2", "dwh_falcon_3","dwh_g_publisher_1","dwh_g_publisher_2","dwh_g_publisher_3","dwh_g_publisher_4"
            ,"dwh_g_publisher_5","dwh_g_publisher_7","dwh_g_publisher_9");
    private final FileStringTaskSolver fs;
    @PostConstruct
    public void convertTaskToJson(){
        for(String x:factTables){
            for(String y:schema){
                String jsonTask= JsonUtil.toJson(new TaskJson(x,y));
                fs.createTask(jsonTask);
            }
        }
        fs.readTask();
    }
}
