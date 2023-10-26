package com.example.querygenerate.services;

import com.example.querygenerate.data.Dim;
import com.example.querygenerate.data.Fact;
import com.example.querygenerate.exception.RedshiftException;
import com.example.querygenerate.utils.QueryGenerateUtils;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author QuangNN
 */
@Log4j
@Service
public class RunnerService {
    private static final Map<String, Dim> dimHashMap = new HashMap<>();
    private static final Map<String, String> fieldsMap = new HashMap<>();
    private static final Map<String, Fact> factHashMap = new HashMap<>();
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/*****", "quangnn", "******");
    private static final Random rand = new Random();
    private static final Logger LOGGER = Logger.getLogger(RunnerService.class.getName());
    private static final String DWH_TEST = "dwh_test";

    @PostConstruct
    public void preRun(){
        Gson gson = new Gson();
        Dim[] dimList = gson.fromJson(jsonDim, Dim[].class);
        for (Dim dim : dimList) {
            dimHashMap.put(dim.getTableName(), dim);
        }
        Fact[] factList = gson.fromJson(jsonFact, Fact[].class);
        for (Fact fact : factList) {
            if (!fact.getTableName().contains("fact")) continue;
            factHashMap.put(fact.getTableName(), fact);
        }
    }

    public void createQuery(String factString, String schema, String day) throws SQLException {
        Fact fact = factHashMap.get(factString);
        List<Dim> dims = new ArrayList<>();
        for (String s : fact.getEtlMap().getDimFields().values()) {
            dims.add(dimHashMap.get(s));
        }
        StringBuilder etlCommand = new StringBuilder();
        for (int i = 7; i < fact.getEtlQueryCommand().length(); i++) {
            if (fact.getEtlQueryCommand().startsWith("from", i)) break;
            etlCommand.append(fact.getEtlQueryCommand().charAt(i));
        }
        //split etl command into fields
        String[] fields = etlCommand.toString().split(",");
        for (String field : fields) {
            field = field.trim();
            if (field.contains("as")) {
                String[] lastPartWords = field.split(" ");
                String lastWord = lastPartWords[lastPartWords.length - 1];
                fieldsMap.put(lastWord, field);
            }
        }
        String fact1TableQuery;
        if (schema.equals(DWH_TEST))
            fact1TableQuery = QueryGenerateUtils.generateQueryForFact1TableForTestSchema(fact, schema, day);
        else fact1TableQuery = QueryGenerateUtils.generateQueryForFact1TableForRealSchema(fact, schema, day);
        String fact2TableQuery = QueryGenerateUtils.generateQueryForFact2Table(fact, schema, dimHashMap, day);
        List<String> dimQueries = QueryGenerateUtils.generateQueryForDimTables(dims, schema, fact.getRawTable(), fieldsMap);
        redshiftDC2Service.executeUpdate(fact1TableQuery);
        for (String query : dimQueries) redshiftDC2Service.executeUpdate(query);
        redshiftDC2Service.executeUpdate(fact2TableQuery);
    }
    // public void comparePerformanceBetweenRa3AndDc2Weak() {
    //     List<String>apiTable=new ArrayList<>(Arrays.asList("api_ads_log_raw_data","api_resource_log_raw_data","api_session_raw_data"
    //     ,"api_level_log_raw_data", "api_retention_raw_data","api_funnel_raw_data"));
    //     Map<String, List<String>> days = new HashMap<>();
    //     days.put("api_ads_log_raw_data", Arrays.asList("2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28", "2023-09-29", "2023-09-30", "2023-10-01"));
    //     days.put("api_resource_log_raw_data", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("api_session_raw_data", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("api_level_log_raw_data", Arrays.asList("2023-10-01", "2023-10-03", "2023-10-02"));
    //     days.put("api_retention_raw_data", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("api_funnel_raw_data", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     List<Thread> dc2Threads = new ArrayList<>();
    //     List<Thread> ra3Threads = new ArrayList<>();
    //     Set<String> visited = new HashSet<>();
    //     for (int i = 0; i < 10; i++) {
    //         String fact = apiTable.get(rand.nextInt(days.size()));
    //         List<String> factDay = days.get(fact);
    //         String day = factDay.get(rand.nextInt(factDay.size()));
    //         if (visited.contains(fact + " " + day)) continue;
    //         visited.add(fact + " " + day);
    //         dc2Threads.add(new Thread(() -> {
    //             try {
    //                 createQuery(fact, DWH_TEST, day);
    //             } catch (SQLException e) {
    //                 throw new RedshiftException(e.getMessage());
    //             }
    //         }));
    //         ra3Threads.add(new Thread(() -> {
    //             try {
    //                 createQuery(fact, DWH_TEST, day);
    //             } catch (SQLException e) {
    //                 throw new RedshiftException(e.getMessage());
    //             }
    //         }));
    //     }
    //     long startTime = System.currentTimeMillis();
    //     for (Thread dc2Thread : dc2Threads) {
    //         dc2Thread.start();
    //     }
    //     try {
    //         for (Thread dc2Thread : dc2Threads) dc2Thread.join();
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //     }
    //     long endTime = System.currentTimeMillis();
    //     long executionTime = endTime - startTime;
    //     LOGGER.log(Level.INFO, "total runtime dc2: {0}", executionTime);

    //     startTime = System.currentTimeMillis();
    //     for (Thread ra3Thread : ra3Threads) {
    //         ra3Thread.start();
    //     }

    //     try {
    //         for (Thread ra3Thread : ra3Threads) ra3Thread.join();
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //     }
    //     endTime = System.currentTimeMillis();
    //     executionTime = endTime - startTime;
    //     LOGGER.log(Level.INFO, "total runtime ra3: {0}", executionTime);
    // }

    // public void comparePerformanceBetweenRa3AndDc2() {
    //     Map<String, List<String>> days = new HashMap<>();
    //     days.put("fact_ads_view", Arrays.asList("2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28", "2023-09-29", "2023-09-30", "2023-10-01"));
    //     days.put("fact_session_time", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("fact_resource_log", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("fact_action", Arrays.asList("2023-10-01", "2023-10-03", "2023-10-02"));
    //     days.put("fact_retention", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     days.put("fact_account_ads_view", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
    //     List<Thread> dc2Threads = new ArrayList<>();
    //     List<Thread> ra3Threads = new ArrayList<>();
    //     Set<String> visited = new HashSet<>();
    //     for (int i = 0; i < 15; i++) {
    //         String fact = factTable.get(rand.nextInt(factTable.size()));
    //         List<String> factDay = days.get(fact);
    //         String day = factDay.get(rand.nextInt(factDay.size()));
    //         if (visited.contains(fact + " " + day)) continue;
    //         visited.add(fact + " " + day);
    //         dc2Threads.add(new Thread(() -> {
    //             try {
    //                 createQuery(fact, DWH_TEST, day);
    //             } catch (SQLException e) {
    //                 throw new RedshiftException(e.getMessage());
    //             }
    //         }));
    //         ra3Threads.add(new Thread(() -> {
    //             try {
    //                 createQuery(fact, DWH_TEST, day);
    //             } catch (SQLException e) {
    //                 throw new RedshiftException(e.getMessage());
    //             }
    //         }));
    //     }
    //     long startTime = System.currentTimeMillis();
    //     for (Thread dc2Thread : dc2Threads) {
    //         dc2Thread.start();
    //     }
    //     try {
    //         for (Thread dc2Thread : dc2Threads) dc2Thread.join();
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //     }
    //     long endTime = System.currentTimeMillis();
    //     long executionTime = endTime - startTime;
    //     LOGGER.log(Level.INFO, "total runtime dc2: {0}", executionTime);

    //     startTime = System.currentTimeMillis();
    //     for (Thread ra3Thread : ra3Threads) {
    //         ra3Thread.start();
    //     }

    //     try {
    //         for (Thread ra3Thread : ra3Threads) ra3Thread.join();
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //     }
    //     endTime = System.currentTimeMillis();
    //     executionTime = endTime - startTime;
    //     LOGGER.log(Level.INFO, "total runtime ra3: {0}", executionTime);
    // }
//    public static void testRa3PerformanceInRealSchema(){
//        Map<String, List<String>> days = new HashMap<>();
//        days.put("fact_resource_log", Arrays.asList("2023-08-13", "2023-08-15", "2023-09-27", "2023-09-28", "2023-09-29", "2023-09-30", "2023-10-01", "2023-10-05",
//                "2023-09-24","2023-09-20", "2023-09-09","2023-08-11","2023-08-12","2023-08-16","2023-08-17","2023-08-18","2023-08-19","2023-08-20",
//                "2023-08-27","2023-08-23","2023-08-26","2023-09-26","2023-08-10","2023-08-09"));
//        List<Thread> ra3Threads = new ArrayList<>();
//        Set<String> visited = new HashSet<>();
//        List<String[]>u=new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            List<String> factDay = days.get("fact_resource_log");
//            String day = factDay.get(rand.nextInt(factDay.size()));
//            if (visited.contains("fact_resource_log" + " " + day)) continue;
//            visited.add("fact_resource_log" + " " + day);
//            ra3Threads.add(new Thread(() -> {
//                try {
//                    createQuery("fact_resource_log", "dwh_falcon_1", day, "Ra3");
//                } catch (SQLException e) {
//                    throw new RuntimeException(e.getMessage());
//                }
//            }));
//
//        }
//        long startTime = System.currentTimeMillis();
//        for (Thread ra3Thread : ra3Threads) {
//            ra3Thread.start();
//        }
//        try {
//            for (Thread ra3Thread : ra3Threads) ra3Thread.join();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        long endTime = System.currentTimeMillis();
//        long executionTime = endTime - startTime;
//        LOGGER.log(Level.INFO, "total runtime ra3: {0}", executionTime);
//    }
}
