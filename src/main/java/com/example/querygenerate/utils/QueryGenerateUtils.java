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
