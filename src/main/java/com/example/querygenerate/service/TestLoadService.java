package com.example.querygenerate.service;
import org.springframework.stereotype.Service;

import java.util.*;


/**
 * @author QuangNN
 */
//@Service
public class TestLoadService {
//    private static final RedshiftService redshiftRA3Service = new RedshiftService("jdbc:redshift://test-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "admin", "mj4Yl9O37GuWxSwLz0Wjs3DJ7");
//
//    public static void stressTestRa3Dwh_1(){
//        Random rand = new Random();
//        List<String> table = Arrays.asList("api_ads_log_raw_data_2023_09_25", "api_ads_log_raw_data_2023_10_02", "api_inapp_log_raw_data_2023_09_25",
//                "api_resource_log_raw_data_2023_08_14", "api_resource_log_raw_data_2023_08_21","api_resource_log_raw_data_2023_09_04");
//        List<Thread> ra3Threads = new ArrayList<>();
//        for (int i = 0; i < 12; i++) {
//            String fact = table.get(rand.nextInt(table.size()));
//            ra3Threads.add(new Thread(() -> {
//                try {
//                    while(true) {
//                        String query = "select * from dwh_falcon_1."+fact+" limit 100000";
//                        String query1 = "select * from dwh_falcon_1."+fact+" limit 100000";
//                        String query2 = "select * from dwh_falcon_1."+fact+" limit 100000";
//                        String query3 = "select * from dwh_falcon_1."+fact+" limit 100000";
//                        String query4 = "select * from dwh_falcon_1."+fact+" limit 100000";
//                        System.out.println(query1);
//                        redshiftRA3Service.executeSelect(query);
//                        redshiftRA3Service.executeSelect(query1);
//                        redshiftRA3Service.executeSelect(query2);
//                        redshiftRA3Service.executeSelect(query3);
//                        redshiftRA3Service.executeSelect(query4);
//                    }
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }));
//        }
//        for (Thread ra3Thread : ra3Threads) {
//            ra3Thread.start();
//        }
//        try {
//            for (Thread ra3Thread : ra3Threads) ra3Thread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//    public static void stressTestRa3Dwh_test(){
//        Random rand = new Random();
//        List<String> table = Arrays.asList("api_ads_log_raw_data", "api_funnel_raw_data", "api_inapp_log_raw_data",
//                "api_level_log_raw_data", "api_property_raw_data","api_resource_log_raw_data");
//        List<Thread> ra3Threads = new ArrayList<>();
//        for (int i = 0; i < 12; i++) {
//            String fact = table.get(rand.nextInt(table.size()));
//            ra3Threads.add(new Thread(() -> {
//                try {
//                    while(true) {
//                        String query = "select  * from dwh_test."+fact+" limit 100000";
//                        String query1 = "select * from dwh_test."+fact+" limit 100000";
//                        String query2 = "select * from dwh_test."+fact+" limit 100000";
//                        String query3 = "select * from dwh_test."+fact+" limit 100000";
//                        String query4 = "select * from dwh_test."+fact+" limit 100000";
//                        System.out.println(query);
//                        redshiftRA3Service.executeSelect(query);
//                        redshiftRA3Service.executeSelect(query1);
//                        redshiftRA3Service.executeSelect(query2);
//                        redshiftRA3Service.executeSelect(query3);
//                        redshiftRA3Service.executeSelect(query4);
//                    }
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }));
//        }
//        for (Thread ra3Thread : ra3Threads) {
//            ra3Thread.start();
//        }
//        try {
//            for (Thread ra3Thread : ra3Threads) ra3Thread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//    public static void main(String[] args) {
//        stressTestRa3Dwh_test();
//    }
}
