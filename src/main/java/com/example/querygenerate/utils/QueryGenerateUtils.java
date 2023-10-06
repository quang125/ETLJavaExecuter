package com.example.querygenerate.utils;

import com.example.querygenerate.data.Dim;
import com.example.querygenerate.data.Fact;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author QuangNN
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryGenerateUtils {
    public static String generateQueryForFact1Table(Fact fact, String schema, String day){
        // Convert to quoted string format
        String quotedDate = "'" + day + "'";
        String command=fact.getEtlQueryCommand();
        command=command.replace("%s",schema);
        String reformatDay=day.replace("-","_");
        command=command.replace("time_id = ?","created_date_str = "+quotedDate);
        return "Create Table " + schema + "." + fact.getTableName() + "_temp_1_"+reformatDay+" as (" + command + ");";
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
        return query.toString();
    }
}
