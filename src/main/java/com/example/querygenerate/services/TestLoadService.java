package com.example.querygenerate.services;

import java.util.*;


/**
 * @author QuangNN
 */
//@Service
public class TestLoadService {
    private static final RedshiftService redshiftRA3Service = new RedshiftService("jdbc:redshift://test-dc2-cluster-2.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "admin", "mj4Yl9O37GuWxSwLz0Wjs3DJ7");

    public static void stressTestRa3Dwh_1(){
        Random rand = new Random();
        List<String> table = Arrays.asList("api_ads_log_raw_data", "api_ads_log_raw_data", "api_inapp_log_raw_data",
                "api_resource_log_raw_data", "api_resource_log_raw_data","api_resource_log_raw_data");
        List<Thread> ra3Threads = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            String fact = table.get(rand.nextInt(table.size()));
            ra3Threads.add(new Thread(() -> {
                try {
                    while(true) {
                        String fact1 = table.get(rand.nextInt(table.size()));
                        String query = "select  * from dwh_test."+fact1+" limit 100000";
                        String fact2 = table.get(rand.nextInt(table.size()));
                        String query1 = "select * from dwh_test."+fact2+" limit 100000";
                        String fact3 = table.get(rand.nextInt(table.size()));
                        String query2 = "select * from dwh_test."+fact3+" limit 100000";
                        String fact4 = table.get(rand.nextInt(table.size()));
                        String query3 = "select * from dwh_test."+fact4+" limit 100000";
                        String fact5 = table.get(rand.nextInt(table.size()));
                        String query4 = "select * from dwh_test."+fact5+" limit 100000";
                        System.out.println(query);
                        redshiftRA3Service.executeSelect(query);
                        redshiftRA3Service.executeSelect(query1);
                        redshiftRA3Service.executeSelect(query2);
                        redshiftRA3Service.executeSelect(query3);
                        redshiftRA3Service.executeSelect(query4);
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
    public static void stressTestRa3Dwh_test(){
        Random rand = new Random();
        List<String> table = Arrays.asList("api_ads_log_raw_data", "api_funnel_raw_data", "api_inapp_log_raw_data",
                "api_level_log_raw_data", "api_property_raw_data","api_resource_log_raw_data");
        List<Thread> ra3Threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ra3Threads.add(new Thread(() -> {
                try {
                    while(true) {
                        String fact1 = table.get(rand.nextInt(table.size()));
                        String query = "select  * from dwh_test."+fact1+" limit 400000";
                        String fact2 = table.get(rand.nextInt(table.size()));
                        String query1 = "select * from dwh_test."+fact2+" limit 400000";
                        String fact3 = table.get(rand.nextInt(table.size()));
                        String query2 = "select * from dwh_test."+fact3+" limit 400000";
                        String fact4 = table.get(rand.nextInt(table.size()));
                        String query3 = "select * from dwh_test."+fact4+" limit 400000";
                        String fact5 = table.get(rand.nextInt(table.size()));
                        String query4 = "select * from dwh_test."+fact5+" limit 400000";
                        System.out.println(query);
                        redshiftRA3Service.executeSelect(query);
                        redshiftRA3Service.executeSelect(query1);
                        redshiftRA3Service.executeSelect(query2);
                        redshiftRA3Service.executeSelect(query3);
                        redshiftRA3Service.executeSelect(query4);
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
}
