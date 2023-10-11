package com.example.querygenerate.utils;

import com.example.querygenerate.data.Dim;
import com.example.querygenerate.data.Fact;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author QuangNN
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryGenerateUtils {
    private static final LocalDate firstBackupDate=LocalDate.parse("2023-08-14");
    public static String generateQueryForFact1TableForTestSchema(Fact fact, String schema, String day){
        LocalDate date=LocalDate.parse(day);
        long dayDiff= ChronoUnit.DAYS.between(firstBackupDate, date);
        // Convert to quoted string format
        String quotedDate = "'" + day + "'";
        String command=fact.getEtlQueryCommand();
        command=command.replace("%s",schema);
        String reformatDay=day.replace("-","_");
        command=command.replace("time_id = ?","created_date_str = "+quotedDate);
        return "Create Table " + schema + "." + fact.getTableName() + "_temp_1_"+reformatDay+" as (" + command + ");";
    }

    public static String generateQueryForFact1TableForRealSchema(Fact fact, String schema, String day){
        LocalDate date=LocalDate.parse(day);
        long dayDiff= ChronoUnit.DAYS.between(firstBackupDate, date);
        String quotedDate = "'" + day + "'";
        String command=fact.getEtlQueryCommand();
        command=command.replace("%s",schema);
        String reformatDay=day.replace("-","_");
        LocalDate backupTarget=firstBackupDate.plusDays((dayDiff/7+1)*7);
        command=command.replace("time_id = ?","created_date_str = "+quotedDate);
        command=command.replace("raw_data","raw_data_"+backupTarget.toString().replace("-","_"));
        if(dayDiff%7!=0){
            return "Create Table " + schema + "." + fact.getTableName() + "_temp_1_"+reformatDay+" as (" + command + ");";
        }
        String command1=fact.getEtlQueryCommand();
        command1=command1.replace("%s",schema);
        command1=command1.replace("time_id = ?","created_date_str = "+quotedDate);
        command1=command1.replace("raw_data","raw_data_"+ day.replace("-","_"));
        return "Create Table " + schema + "." + fact.getTableName() + "_temp_1_"+reformatDay+" as ("+command+"\n union "+command1+")";
    }
    public static List<String> generateQueryForDimTables(List<Dim> dimList, String schema, String rawTable, Map<String,String>fieldsMap){
        List<String>queries=new ArrayList<>();
        for(Dim dim:dimList){
            StringBuilder query= new StringBuilder("insert into " + schema + "." + dim.getTableName() + "(id, ");
            for(int i=0;i<dim.getColumnList().size()-1;i++){
                query.append(dim.getColumnList().get(i));
                query.append(",");
            }
            query.append(dim.getColumnList().get(dim.getColumnList().size()-1));
            query.append(")\nwith cte1 ");
            query.append("as (\n" + " SELECT ");
            for(int i=0;i<dim.getColumnList().size()-1;i++){
                String field=dim.getColumnList().get(i);
                query.append(fieldsMap.getOrDefault(field, field));
                query.append(", ");
            }
            String field=dim.getColumnList().get(dim.getColumnList().size()-1);
            query.append(fieldsMap.getOrDefault(field, field));
            query.append("\n from ").append(schema).append(".").append(rawTable).append("\n").append("group by ");
            for(int i=1;i<dim.getColumnList().size();i++){
                query.append(i).append(" ");
                query.append(", ");
            }
            query.append(dim.getColumnList().size()).append(" ");
            query.append("), cte2 as (\nselect ");
            for(int i=0;i<dim.getColumnList().size();i++){
                query.append("a.").append(dim.getColumnList().get(i)).append(", ");
            }
            query.append("d.id\nfrom cte1 a\nleft join ").append(schema).append(".").append(dim.getTableName()).append(" d\non ");
            for(int i=0;i<dim.getColumnList().size()-1;i++){
                query.append("a.").append(dim.getColumnList().get(i)).append(" = ");
                query.append("d.").append(dim.getColumnList().get(i));
                query.append(" and ");
            }
            query.append("a.").append(dim.getColumnList().get(dim.getColumnList().size()-1)).append(" = ");
            query.append("d.").append(dim.getColumnList().get(dim.getColumnList().size()-1));
            query.append("\n), cte3 as (\n  select row_number() over (order by cte2.game_id) as id, ");
            for(int i=0;i<dim.getColumnList().size()-1;i++){
                query.append("cte2.").append(dim.getColumnList().get(i));
                query.append(", ");
            }
            query.append("cte2.").append(dim.getColumnList().get(dim.getColumnList().size()-1));
            query.append("\nfrom cte2 where cte2.id is null\n)\nselect (select max(id) from ").append(schema).append(".").append(dim.getTableName()).append(") + cte3.id,");
            for(int i=0;i<dim.getColumnList().size()-1;i++){
                query.append("cte3.").append(dim.getColumnList().get(i));
                query.append(", ");
            }
            query.append("cte3.").append(dim.getColumnList().get(dim.getColumnList().size()-1));
            query.append("\n from cte3;");
            queries.add(query.toString());
        }
        return queries;
    }
    public static String generateQueryForFact2Table(Fact fact, String schema, Map<String, Dim>dimMap, String day){
        StringBuilder query= new StringBuilder("create table " + schema + "." + fact.getTableName() + "_temp_2_" + day.replace("-", "_") + " as\n(\n  select ");
        Map<String,String>dimFields=fact.getEtlMap().getDimFields();
        List<String>dataFields=fact.getEtlMap().getDataFields();
        int count=1;
        for(String field:dimFields.keySet()){
            query.append("d").append(count).append(".id as ").append(field).append(", ");
            count+=1;
        }
        for(int i=0;i<dataFields.size();i++){
            query.append("t.").append(dataFields.get(i));
            if(i<dataFields.size()-1) query.append(", ");
        }
        query.append("\nfrom ").append(schema).append(".").append(fact.getTableName()).append("_temp_1_").append(day.replace("-", "_")).append(" t");
        count=1;
        for(Map.Entry<String,String> field:dimFields.entrySet()){
            query.append("\njoin ").append(schema).append(".").append(field.getValue()).append(" d").append(count).append("\non ");
            Dim dim=dimMap.get(field.getValue());
            for(int i=0;i<dim.getColumnList().size();i++){
                query.append("t.").append(dim.getColumnList().get(i)).append(" = ");
                query.append("d").append(count).append(".").append(dim.getColumnList().get(i));
                if(i<dim.getColumnList().size()-1) query.append(" and ");
            }
            count+=1;
        }
        query.append("\ngroup by ");
        for(int i=1;i<=dimFields.size()+dataFields.size();i++){
            query.append(i);
            if(i<dimFields.size()+dataFields.size()) query.append(", ");
        }
        query.append("\n);");
        return query.toString().replace(schema+"."+"dim_ab_testing","(select max(id) as id, game_id, ab_testing_id, ab_testing_value from "+ schema+".dim_ab_testing group by 2,3,4) as");
    }
}
/*

--{\"dimFields\":{\"country\":\"dim_country\",\"ads_where\":\"dim_ads_where\",\
--"app_version\":\"dim_app_version\",\"ads_type\":\"dim_ads_type\",
--\"ab_testing_id\":\"dim_ab_testing\",\"game_id\":\"dim_game\"},
--\"dataFields\":[\"account_id\",\"retention_day\",\"time_id\",\"level\",\"install_day\",\"sum_value\",\"created_day\"]}\r\n

--create fact 1

Create Table dwh_test.fact_account_ads_view_temp_1_2023_10_01 as (
select game_id||'_'||platform as game_id, created_date_str as created_day, country, app_version, ab_testing_variable as ab_testing_id, ab_testing_value,
retention_day, time_id, install_day, account_id, type as ads_type,level, ad_where as ads_where, count(1) as sum_value
from dwh_test.api_ads_log_raw_data
where created_date_str = '2023-10-01'
group by game_id, platform, created_date_str, country, app_version, ab_testing_variable, ab_testing_value,
retention_day, time_id, install_day, account_id, ads_type, ads_where, level);


--create fact 2

create table dwh_test.fact_account_ads_view_temp_2_2023_10_01 as
(
  select d1.id as country, d2.id as ads_where, d3.id as app_version, d4.id as ads_type, d5.id as ab_testing_id, d6.id as game_id, t.account_id, t.retention_day, t.time_id, t.level, t.install_day, t.sum_value, t.created_day
from dwh_test.fact_account_ads_view_temp_1_2023_10_01 t
join dwh_test.dim_country d1
on t.game_id = d1.game_id and t.country = d1.country
join dwh_test.dim_ads_where d2
on t.game_id = d2.game_id and t.ads_where = d2.ads_where
join dwh_test.dim_app_version d3
on t.game_id = d3.game_id and t.app_version = d3.app_version
join dwh_test.dim_ads_type d4
on t.game_id = d4.game_id and t.ads_type = d4.ads_type
join (select max(id) as id, game_id, ab_testing_id, ab_testing_value from dwh_test.dim_ab_testing group by 2,3,4) as d5
on t.game_id = d5.game_id and t.ab_testing_id = d5.ab_testing_id and t.ab_testing_value = d5.ab_testing_value
join dwh_test.dim_game d6
on t.game_id = d6.game_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
);

--create fact 3

create table dwh_test.fact_account_ads_view_temp_3_2023_10_01 as(
select d1.country, d2.ads_where, d3.app_version, d4.ads_type, d5.ab_testing_id, d5.ab_testing_value, d6.game_id, t.account_id, t.retention_day, t.time_id, t.level, t.install_day, t.sum_value, t.created_day
from dwh_test.fact_account_ads_view_temp_2_2023_10_01 t
join dwh_test.dim_country d1
on t.country=d1.id
join dwh_test.dim_ads_where d2
on t.ads_where=d2.id
join dwh_test.dim_app_version d3
on t.app_version=d3.id
join dwh_test.dim_ads_type d4
on t.ads_type=d4.id
join dwh_test.dim_ab_testing d5
on t.ab_testing_id=d5.id
join dwh_test.dim_game d6
on t.game_id=d6.id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

--check 3 conditions

select count(*) from dwh_test.fact_account_ads_view_temp_1_2023_10_01

select count(*) from dwh_test.fact_account_ads_view_temp_3_2023_10_01

select count(*) from
(
 (
  SELECT t1.country, t1.ads_where, t1.app_version, t1.ads_type, t1.ab_testing_id, t1.ab_testing_value, t1.game_id, t1.account_id, t1.retention_day, t1.time_id, t1.level,
  t1.install_day, t1.sum_value, t1.created_day FROM dwh_test.fact_account_ads_view_temp_1_2023_10_01 t1
 )
 UNION
 (
  SELECT t1.country, t1.ads_where, t1.app_version, t1.ads_type, t1.ab_testing_id, t1.ab_testing_value, t1.game_id, t1.account_id, t1.retention_day, t1.time_id, t1.level,
  t1.install_day, t1.sum_value, t1.created_day FROM dwh_test.fact_account_ads_view_temp_3_2023_10_01 t1
 )
)


 */