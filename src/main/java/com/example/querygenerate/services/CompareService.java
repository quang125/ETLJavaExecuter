package com.example.querygenerate.services;

import com.example.querygenerate.data.Dim;
import com.example.querygenerate.data.Fact;
import com.example.querygenerate.utils.QueryGenerateUtils;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author QuangNN
 */
@Service
public class CompareService {
    private static final Map<String, Dim> dimHashMap = new HashMap<>();
    private static final Map<String, String> fieldsMap = new HashMap<>();
    private static final Map<String, Fact> factHashMap = new HashMap<>();
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/********", "quangnn", "***********");
    private static final RedshiftService redshiftRA3Service = new RedshiftService("jdbc:redshift://test-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/******", "admin", "**********");
    private static final Logger LOGGER = Logger.getLogger(CompareService.class.getName());
    private static final Map<String,List<String>> factTableDays = new HashMap<>();
    private static final Map<String,List<String>> factTableFields = new HashMap<>();
    @PostConstruct
    public static void preRun() {
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
        List<String>days=new ArrayList<>(Arrays.asList("2023-10-02","2023-10-03","2023-10-04"));
        factTableDays.put("fact_session_time",days);
        factTableDays.put("fact_ads_view",days);
        factTableDays.put("fact_resource_log",days);
        factTableFields.put("fact_session_time",Arrays.asList("created_day","country","app_version","mode","level","account_id","sum_value","retention_day","time_id","install_day","session_id"));
        factTableFields.put("fact_ads_view",Arrays.asList("created_day","country","app_version","level","ads_type","ads_where","sum_value","retention_day","time_id","install_day"));
        factTableFields.put("fact_resource_log",Arrays.asList("created_day","country","app_version","level","sum_value","retention_day","time_id","resource_item_type","install_day"));
        factTableFields.put("fact_level_daily_time_play",Arrays.asList("created_day","country","app_version","ab_testing_id","level_level","level_difficulty","level_status","count_value","sum_value","retention_day","time_id","install_day"));
        factTableFields.put("fact_funnel",Arrays.asList("created_day","country","app_version","level","sum_value","retention_day","time_id","install_day","priority","action","funnel_id","funnel_day"));
        factTableFields.put("fact_retention",Arrays.asList("created_day","country","app_version","level","sum_value","retention_day","time_id","day","install_day"));
        factTableFields.put("fact_action", Arrays.asList("created_day","country","app_version","level","sum_value","retention_day","time_id","install_day","priority","a_id"));
        factTableFields.put("fact_property",Arrays.asList("created_day","country","app_version","level","sum_value","retention_day","time_id","install_day","priority","p_name","p_value","account_id"));
        factTableFields.put("fact_inapp", Arrays.asList("created_day","country","app_version","inapp_where","level","sum_value","retention_day","time_id","price","install_day","account_id","price_usd"));
    }

    public boolean compareTemp1VsTemp3(String factString, String schema, String day, String redshiftService) throws SQLException {
        System.out.println(factString + " " + day);
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
        String fact3TableQuery = QueryGenerateUtils.generateQueryForFact3Table(fact, schema, dimHashMap, day);
        redshiftDC2Service.executeUpdate(fact3TableQuery);
        List<String> queries = QueryGenerateUtils.generateCompareCountQueriesFact1AndFact2(fact, schema, dimHashMap, day);
        boolean check=checkCountEqual(queries);
        String q = "drop table " + schema + "." + factString + "_temp_3_" + day.replace("-", "_");
        redshiftDC2Service.executeUpdate(q);
        if (!check) {
            LOGGER.log(Level.INFO, "different number of records at table {0}", "check fact 1 "+factString + "_" + day.replace("-", "_"));
        }
        LOGGER.log(Level.INFO, "same number of records at table {0}", "check fact 1 "+factString + "_" + day.replace("-", "_"));
        return true;
    }
    public void compareTemp2vsRealFactTable() throws SQLException {
        for(String fact:factTableFields.keySet()){
            for(String day:factTableDays.get(fact)){
                System.out.println(fact+" "+day);
                List<String>queries=QueryGenerateUtils.generateCompareCountQueriesFact2AndFactReal(fact,"dwh_test",day,factTableFields.get(fact),"Ra3");
                boolean check=checkCountEqual(queries);
                if (!check) {
                    LOGGER.log(Level.INFO, "different number of records at table {0}", "check fact 2 "+fact + "_" + day.replace("-", "_"));
                }
                else LOGGER.log(Level.INFO, "same number of records at table {0}", "check fact 2 "+fact + "_" + day.replace("-", "_"));
            }
        }
    }

    public static boolean checkCountEqual(List<String>queries) throws SQLException {
        Set<String> value = new HashSet<>();
        for (String query : queries) {
            value.add(redshiftDC2Service.executeCountSelect(query));
        }
        if (!(value.size() == 1)) {
            return false;
        }
        return true;
    }

}
