package com.example.querygenerate.service;

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
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "quangnn", "Yvx83kfRmHt42b6kqgM5gzjG6");
    //private static final RedshiftService redshiftRA3Service = new RedshiftService("jdbc:redshift://test-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "admin", "mj4Yl9O37GuWxSwLz0Wjs3DJ7");
    private static final Random rand = new Random();
    private static final Logger LOGGER = Logger.getLogger(RunnerService.class.getName());
    private static final String DWH_TEST = "dwh_test";
    private static final List<String> factTable = Arrays.asList("fact_ads_view", "fact_resource_log", "fact_action", "fact_retention", "fact_account_ads_view", "fact_session_time");
    static Scanner sc = new Scanner(System.in);

    @PostConstruct
    public static void preRun() {
        Gson gson = new Gson();
        String jsonDim = "[{\"tableName\":\"dim_country\",\"keyColumn\":\"[\\\"game_id\\\",\\\"country\\\"]\",\"id\":\"6361f9800b35c0bf3d57e709\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_app_version\",\"keyColumn\":\"[\\\"game_id\\\",\\\"app_version\\\"]\",\"id\":\"6361f9800b35c0bf3d57e708\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_ab_testing\",\"keyColumn\":\"[\\\"game_id\\\",\\\"ab_testing_id\\\",\\\"ab_testing_value\\\"]\",\"id\":\"6361f9800b35c0bf3d57e705\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_property\",\"keyColumn\":\"[\\\"game_id\\\",\\\"p_name\\\"]\",\"id\":\"641bbdcf1e65304e34dc009b\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_game\",\"keyColumn\":\"[\\\"game_id\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70a\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_inapp_where\",\"keyColumn\":\"[\\\"game_id\\\",\\\"inapp_where\\\"]\",\"id\":\"637c814190d19c56defbe519\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_mode\",\"keyColumn\":\"[\\\"game_id\\\",\\\"mode\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70d\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_funnel\",\"keyColumn\":\"[\\\"game_id\\\",\\\"funnel_name\\\"]\",\"id\":\"637c3ed7e15f4d58ba093669\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_resource_item_type\",\"keyColumn\":\"[\\\"game_id\\\",\\\"flow_type\\\",\\\"currency\\\",\\\"item_type\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70f\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_action\",\"keyColumn\":\"[\\\"game_id\\\",\\\"a_to\\\",\\\"a_from\\\"]\",\"id\":\"641bbdba1e65304e34dc009a\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_ads_type\",\"keyColumn\":\"[\\\"game_id\\\",\\\"ads_type\\\"]\",\"id\":\"6361f9800b35c0bf3d57e706\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_inapp_product_id\",\"keyColumn\":\"[\\\"game_id\\\",\\\"product_id\\\"]\",\"id\":\"637c814c90d19c56defbe51a\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_level_difficulty\",\"keyColumn\":\"[\\\"game_id\\\",\\\"app_version\\\",\\\"level_difficulty\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70c\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_level\",\"keyColumn\":\"[\\\"game_id\\\",\\\"level\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70b\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_ads_where\",\"keyColumn\":\"[\\\"game_id\\\",\\\"ads_where\\\"]\",\"id\":\"6361f9800b35c0bf3d57e707\",\"tableConfigName\":\"dwh_config_table\"},{\"tableName\":\"dim_resource_currency\",\"keyColumn\":\"[\\\"game_id\\\",\\\"currency\\\"]\",\"id\":\"6361f9800b35c0bf3d57e70e\",\"tableConfigName\":\"dwh_config_table\"}]\n";
        Dim[] dimList = gson.fromJson(jsonDim, Dim[].class);
        for (Dim dim : dimList) {
            dimHashMap.put(dim.getTableName(), dim);
        }
        String jsonFact = "[{\"tableName\":\"fact_uninstall\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":10,\"checkColumn\":\"\",\"etlThreadNumber\":3,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, account_id,ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, uninstall_date_str,level, count(1) as sum_value from %s.api_uninstall_raw_data  where time_id \\u003d ? group by game_id, account_id,platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, uninstall_date_str,level\\r\\n\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_uninstall_raw_data  where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"level\\\",\\\"sum_value\\\",\\\"created_day\\\", \\\"uninstall_date_str\\\",\\\"account_id\\\"]}\",\"rawTable\":\"api_uninstall_raw_data\",\"id\":\"647573efb8de274ebfef2bda\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-05-30\"},{\"tableName\":\"api_uninstall_raw_data\",\"timeStep\":30,\"backupType\":1,\"timeVacuum\":5,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"6475742bb8de274ebfef2bdb\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-05-30\"},{\"tableName\":\"fact_level_daily_time_play\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":5,\"checkColumn\":\"\",\"etlThreadNumber\":4,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, level as level_level, difficulty as level_difficulty, status as level_status,count(1) as count_value,sum(duration) as sum_value\\r\\nfrom %s.api_level_log_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, level, difficulty, status\",\"etlQueryCheckCommand\":\"select SUM(duration) as check_value from %s.api_level_log_raw_data allrd  where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"level_difficulty\\\":\\\"dim_level_difficulty\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"created_day\\\",\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"level_level\\\",\\\"level_status\\\",\\\"count_value\\\",\\\"sum_value\\\"]}\\r\\n\",\"rawTable\":\"api_level_log_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e725\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-12\"},{\"tableName\":\"fact_inapp\",\"timeStep\":2,\"backupType\":0,\"timeVacuum\":7,\"checkColumn\":\"\",\"etlThreadNumber\":5,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, account_id, inapp_where , level, price ,price_usd, product_id , count(1) as sum_value\\r\\nfrom %s.api_inapp_log_raw_data \\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, account_id, inapp_where , level,price ,price_usd, product_id \",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_inapp_log_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"product_id\\\":\\\"dim_inapp_product_id\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"inapp_where\\\":\\\"dim_inapp_where\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"account_id\\\", \\\"sum_value\\\",\\\"created_day\\\",\\\"level\\\",\\\"price\\\",\\\"price_usd\\\"]}\\r\\n\",\"rawTable\":\"api_inapp_log_raw_data\",\"id\":\"637c76ce90d19c56defbe517\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-12\"},{\"tableName\":\"fact_property\",\"timeStep\":1,\"backupType\":0,\"timeVacuum\":0,\"checkColumn\":\"\",\"etlThreadNumber\":5,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, account_id,ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, p_name, p_priority as priority, p_value,level, count(1) as sum_value\\r\\nfrom %s.api_property_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, account_id,platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, p_name, priority, p_value,level\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_property_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"p_name\\\":\\\"dim_property\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"p_value\\\",\\\"priority\\\",\\\"level\\\",\\\"sum_value\\\",\\\"created_day\\\", \\\"account_id\\\"]}\",\"rawTable\":\"api_property_raw_data\",\"id\":\"6421174c1e65304e34dc00a1\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-03-27\"},{\"tableName\":\"fact_level_difficulty\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":6,\"checkColumn\":\"\",\"etlThreadNumber\":5,\"etlQueryCommand\":\"select\\r\\n   created_date_str as created_day,\\r\\n   game_id,\\r\\n   country,\\r\\n   app_version,\\r\\n   ab_testing_variable as ab_testing_id,\\r\\n   ab_testing_value,\\r\\n   level_level,\\r\\n   level_difficulty,\\r\\n   account_id,\\r\\n   retention_day,\\r\\n   time_id,\\r\\n   install_day,\\r\\n   total_fail,\\r\\n   1 as sum_value,\\r\\n   case when total_pass \\u003d 0 then 0 else 1 end as is_passed from(\\r\\n   select\\r\\n   created_date_str,\\r\\n   game_id||\\u0027_\\u0027||platform as game_id,\\r\\n   country,\\r\\n   app_version,\\r\\n   ab_testing_variable,\\r\\n   ab_testing_value,\\r\\n   level as level_level,\\r\\n   difficulty as level_difficulty,\\r\\n   account_id,\\r\\n   retention_day,\\r\\n   time_id,\\r\\n   install_day,\\r\\n   sum(case when status \\u003d \\u0027pass\\u0027 then 0 else 1 end ) as total_fail,\\r\\n   sum(case when status \\u003d \\u0027pass\\u0027 then 1 else 0 end ) as total_pass\\r\\n   from %s.api_level_log_raw_data\\r\\n   where time_id \\u003d ?\\r\\n   and status in (\\u0027pass\\u0027, \\u0027fail\\u0027)\\r\\n   group by created_date_str,\\r\\n   game_id,\\r\\n   platform,\\r\\n   country,\\r\\n   app_version,\\r\\n   ab_testing_variable,\\r\\n   ab_testing_value,\\r\\n   level,\\r\\n   difficulty,\\r\\n   account_id,\\r\\n  retention_day,\\r\\n  time_id,\\r\\n  install_day)\\r\\n  \",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"level_difficulty\\\":\\\"dim_level_difficulty\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"level_level\\\",\\\"account_id\\\",\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"created_day\\\",\\\"sum_value\\\",\\\"is_passed\\\",\\\"total_fail\\\"]}\\r\\n\",\"rawTable\":\"api_level_log_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e72b\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"api_session_raw_data\",\"timeStep\":15,\"backupType\":1,\"timeVacuum\":3,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e72a\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"api_action_raw_data\",\"timeStep\":360,\"backupType\":1,\"timeVacuum\":11,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"641bb0ff1e65304e34dc0099\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-03-23\"},{\"tableName\":\"fact_account_ads_view\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":7,\"checkColumn\":\"\",\"etlThreadNumber\":2,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, \\r\\nretention_day, time_id, install_day, account_id, type as ads_type,level, ad_where as ads_where, count(1) as sum_value\\r\\nfrom %s.api_ads_log_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, \\r\\nretention_day, time_id, install_day, account_id, ads_type, ads_where, level\",\"etlQueryCheckCommand\":\"select count(*)  as check_value from %s.api_ads_log_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"ads_where\\\":\\\"dim_ads_where\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"ads_type\\\":\\\"dim_ads_type\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"account_id\\\",\\\"retention_day\\\",\\\"time_id\\\",\\\"level\\\",\\\"install_day\\\",\\\"sum_value\\\",\\\"created_day\\\"]}\\r\\n\",\"rawTable\":\"api_ads_log_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e723\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-12\"},{\"tableName\":\"fact_session_time\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":2,\"checkColumn\":\"\",\"etlThreadNumber\":5,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, game_mode as mode,session_id, level, account_id,sum(session_time) as sum_value\\r\\nfrom %s.api_session_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, game_mode,session_id, level, account_id\",\"etlQueryCheckCommand\":\"select SUM(session_time)  as check_value from %s.api_session_raw_data  where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"mode\\\":\\\"dim_mode\\\",\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"level\\\",\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"created_day\\\",\\\"sum_value\\\",\\\"account_id\\\", \\\"session_id\\\"]}\",\"rawTable\":\"api_session_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e727\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"fact_resource_log\",\"timeStep\":5,\"backupType\":0,\"timeVacuum\":4,\"checkColumn\":\"\",\"etlThreadNumber\":10,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, level, flow_type, item_type, currency, sum(amount) as sum_value \\r\\nfrom %s.api_resource_log_raw_data arlrd \\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, level, account_id, flow_type , item_type , currency\",\"etlQueryCheckCommand\":\"select SUM(amount)  as check_value from %s.api_resource_log_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"resource_currency\\\":\\\"dim_resource_currency\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"resource_item_type\\\":\\\"dim_resource_item_type\\\"},\\\"dataFields\\\":[\\\"level\\\",\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"created_day\\\",\\\"sum_value\\\"]}\",\"rawTable\":\"api_resource_log_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e726\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"api_ads_log_raw_data\",\"timeStep\":10,\"backupType\":1,\"timeVacuum\":4,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e728\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"fact_retention\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":7,\"checkColumn\":\"\",\"etlThreadNumber\":2,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day ,level, \\\"day\\\" , count(distinct account_id) as sum_value\\r\\nfrom %s.api_retention_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day , level,\\\"day\\\"\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_retention_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\", \\\"day\\\",\\\"level\\\",\\\"sum_value\\\",\\\"created_day\\\"]}\\r\\n\",\"rawTable\":\"api_retention_raw_data\",\"id\":\"637c7db190d19c56defbe518\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-12\"},{\"tableName\":\"api_level_log_raw_data\",\"timeStep\":15,\"backupType\":1,\"timeVacuum\":3,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e72f\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"fact_ads_view\",\"timeStep\":2,\"backupType\":0,\"timeVacuum\":3,\"checkColumn\":\"\",\"etlThreadNumber\":5,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, account_id,level, type as ads_type, ad_where as ads_where, count(1) as sum_value from %s.api_ads_log_raw_data where time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, account_id, ads_type, ads_where,level\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_ads_log_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"ads_where\\\":\\\"dim_ads_where\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"ads_type\\\":\\\"dim_ads_type\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\",\\\"max_level\\\":\\\"dim_level\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"level\\\",\\\"sum_value\\\",\\\"created_day\\\"]}\\r\\n\",\"rawTable\":\"api_ads_log_raw_data\",\"id\":\"637c74e9eeb88f2f48e86273\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-11-22\"},{\"tableName\":\"api_funnel_raw_data\",\"timeStep\":360,\"backupType\":1,\"timeVacuum\":5,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e72c\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"fact_action\",\"timeStep\":3,\"backupType\":0,\"timeVacuum\":7,\"checkColumn\":\"\",\"etlThreadNumber\":2,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, a_from, a_priority as priority, a_to,level, a_time, count(1) as sum_value\\r\\nfrom %s.api_action_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, a_from, a_time, a_priority , a_to,level\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_action_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"a_id\\\":\\\"dim_action\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"priority\\\",\\\"level\\\",\\\"sum_value\\\",\\\"created_day\\\"]}\\r\\n\",\"rawTable\":\"api_action_raw_data\",\"id\":\"641bc2341e65304e34dc009d\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-03-23\"},{\"tableName\":\"api_resource_log_raw_data\",\"timeStep\":10,\"backupType\":1,\"timeVacuum\":3,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e72e\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"api_property_raw_data\",\"timeStep\":360,\"backupType\":1,\"timeVacuum\":10,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"641bb08c1e65304e34dc0098\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2023-03-23\"},{\"tableName\":\"fact_funnel\",\"timeStep\":2,\"backupType\":0,\"timeVacuum\":6,\"checkColumn\":\"\",\"etlThreadNumber\":2,\"etlQueryCommand\":\"select game_id||\\u0027_\\u0027||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value, retention_day, time_id, install_day, funnel_name , priority, action,level, funnel_day, count(1) as sum_value\\r\\nfrom %s.api_funnel_raw_data\\r\\nwhere time_id \\u003d ?\\r\\ngroup by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value, retention_day, time_id, install_day, funnel_name , funnel_day, priority, action ,level\",\"etlQueryCheckCommand\":\"select count(*) as check_value from %s.api_funnel_raw_data where time_id \\u003d ? and game_id \\u003d ?\",\"etlMap\":\"{\\\"dimFields\\\":{\\\"country\\\":\\\"dim_country\\\",\\\"app_version\\\":\\\"dim_app_version\\\",\\\"funnel_id\\\":\\\"dim_funnel\\\",\\\"ab_testing_id\\\":\\\"dim_ab_testing\\\",\\\"game_id\\\":\\\"dim_game\\\"},\\\"dataFields\\\":[\\\"retention_day\\\",\\\"time_id\\\",\\\"install_day\\\",\\\"action\\\",\\\"priority\\\",\\\"level\\\",\\\"funnel_day\\\",\\\"sum_value\\\",\\\"created_day\\\"]}\\r\\n\",\"rawTable\":\"api_funnel_raw_data\",\"id\":\"63620f7f0b35c0bf3d57e724\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-12\"},{\"tableName\":\"api_retention_raw_data\",\"timeStep\":30,\"backupType\":1,\"timeVacuum\":4,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e729\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"},{\"tableName\":\"api_inapp_log_raw_data\",\"timeStep\":360,\"backupType\":1,\"timeVacuum\":4,\"checkColumn\":\"\",\"etlThreadNumber\":0,\"etlQueryCommand\":\"\",\"etlQueryCheckCommand\":\"\",\"etlMap\":\"\",\"rawTable\":\"\",\"id\":\"63620f7f0b35c0bf3d57e72d\",\"tableConfigName\":\"dwh_config_table\",\"createdDateStr\":\"2022-10-24\"}]\n";

        Fact[] factList = gson.fromJson(jsonFact, Fact[].class);
        for (Fact fact : factList) {
            if (!fact.getTableName().contains("fact")) continue;
            factHashMap.put(fact.getTableName(), fact);
        }
    }

    public void createQuery(String factString, String schema, String day, String redshiftService) throws SQLException {
        //System.out.println(factString+" "+day);
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
        System.out.println(fact1TableQuery);
        //redshiftDC2Service.executeUpdate(fact1TableQuery);
        for (String query : dimQueries) redshiftDC2Service.executeUpdate(query);
        System.out.println(fact2TableQuery);
        //redshiftDC2Service.executeUpdate(fact2TableQuery);
//        if (redshiftService.equals("Ra3")) {
//            redshiftRA3Service.executeUpdate(fact1TableQuery);
//            for (String query : dimQueries){
//                redshiftRA3Service.executeUpdate(query);
//            }
//            redshiftRA3Service.executeUpdate(fact2TableQuery);
//
//            String q = "drop table " + schema + "." + factString + "_temp_1_" + day.replace("-", "_");
//            redshiftRA3Service.executeUpdate(q);
//            String j = "drop table " + schema + "." + factString + "_temp_2_" + day.replace("-", "_");
//            redshiftRA3Service.executeUpdate(j);
//        }


//         else {
//            redshiftDC2Service.executeUpdate(fact1TableQuery);
//            for (String query : dimQueries) redshiftDC2Service.executeUpdate(query);
//            redshiftDC2Service.executeUpdate(fact2TableQuery);
//            String q="drop table dwh_test."+factString+"_temp_1_"+day.replace("-","_");
//            redshiftDC2Service.executeUpdate(q);
//            String j="drop table dwh_test."+factString+"_temp_2_"+day.replace("-","_");
//            redshiftDC2Service.executeUpdate(j);
        //     }
    }

    //    public static void comparePerformanceBetweenRa3AndDc2() {
//        Map<String, List<String>> days = new HashMap<>();
//        days.put("fact_ads_view", Arrays.asList("2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28", "2023-09-29", "2023-09-30", "2023-10-01"));
//        days.put("fact_session_time", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
//        days.put("fact_resource_log", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
//        days.put("fact_action", Arrays.asList("2023-10-01", "2023-10-03", "2023-10-02"));
//        days.put("fact_retention", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
//        days.put("fact_account_ads_view", Arrays.asList("2023-10-02", "2023-10-03", "2023-10-04"));
//        List<Thread> dc2Threads = new ArrayList<>();
//        List<Thread> ra3Threads = new ArrayList<>();
//        Set<String> visited = new HashSet<>();
//        for (int i = 0; i < 15; i++) {
//            String fact = factTable.get(rand.nextInt(factTable.size()));
//            List<String> factDay = days.get(fact);
//            String day = factDay.get(rand.nextInt(factDay.size()));
//            if (visited.contains(fact + " " + day)) continue;
//            visited.add(fact + " " + day);
//            dc2Threads.add(new Thread(() -> {
//                try {
//                    createQuery(fact, DWH_TEST, day, "Dc2");
//                } catch (SQLException e) {
//                    throw new RedshiftException(e.getMessage());
//                }
//            }));
//            ra3Threads.add(new Thread(() -> {
//                try {
//                    createQuery(fact, DWH_TEST, day, "Ra3");
//                } catch (SQLException e) {
//                    throw new RedshiftException(e.getMessage());
//                }
//            }));
//        }
//        long startTime = System.currentTimeMillis();
//        for (Thread dc2Thread : dc2Threads) {
//            dc2Thread.start();
//        }
//        try {
//            for (Thread dc2Thread : dc2Threads) dc2Thread.join();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        long endTime = System.currentTimeMillis();
//        long executionTime = endTime - startTime;
//        LOGGER.log(Level.INFO, "total runtime dc2: {0}", executionTime);
//
//        startTime = System.currentTimeMillis();
//        for (Thread ra3Thread : ra3Threads) {
//            ra3Thread.start();
//        }
//
//        try {
//            for (Thread ra3Thread : ra3Threads) ra3Thread.join();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        endTime = System.currentTimeMillis();
//        executionTime = endTime - startTime;
//        LOGGER.log(Level.INFO, "total runtime ra3: {0}", executionTime);
//    }
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
//    public static void main(String[] args) throws SQLException {
//        preRun();
//        createQuery(sc.next(), sc.next(),sc.next(),sc.next());
//    }
    public void print() {
        System.out.println("Runner run");
    }
}
