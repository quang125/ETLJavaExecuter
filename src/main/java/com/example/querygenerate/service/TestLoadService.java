package com.example.querygenerate.service;

import com.example.querygenerate.data.Dim;
import com.example.querygenerate.data.Fact;
import com.example.querygenerate.utils.QueryGenerateUtils;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author QuangNN
 */
public class TestLoadService {
    private static final RedshiftService redshiftRA3Service = new RedshiftService("jdbc:redshift://test-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "admin", "mj4Yl9O37GuWxSwLz0Wjs3DJ7");

    public static void boastLoadRa3(){
        Random rand = new Random();
        List<String> table = Arrays.asList("api_ads_log_raw_data_2023_09_25", "api_ads_log_raw_data_2023_10_02", "api_inapp_log_raw_data_2023_09_25",
                "api_resource_log_raw_data_2023_08_14", "api_resource_log_raw_data_2023_08_21","api_resource_log_raw_data_2023_09_04");
        List<Thread> ra3Threads = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            String fact = table.get(rand.nextInt(table.size()));
            ra3Threads.add(new Thread(() -> {
                try {
                    while(true) {
                        String query = "select * from dwh_falcon_1."+fact+" limit 142";
                        System.out.println(query);
                        redshiftRA3Service.excuteSelect(query);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Thread ra3Thread : ra3Threads) {
            ra3Thread.start();
        }
        try {
            for (Thread ra3Thread : ra3Threads) ra3Thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        boastLoadRa3();
    }
}
