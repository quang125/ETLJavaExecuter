package com.example.querygenerate.solvers.task;

import com.example.querygenerate.data.json.TaskJson;
import com.example.querygenerate.data.json.TaskStatusJson;
import com.example.querygenerate.utils.JsonUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class FileJsonTaskSolver implements TaskSolver {
    private static final String taskFile = "C:\\task\\task_json.txt";
    private static final String currentTaskFile = "C:\\task\\current_json.txt";
    private static final String logErrorFile = "C:\\task\\log_json.txt";

    @Override
    public List<String> readTask() {
        List<String> tasks = new ArrayList<>();
        List<TaskStatusJson>taskStatusJsons=new ArrayList<>();
        try (FileReader fileReader = new FileReader(taskFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
        ) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                TaskJson taskJson= JsonUtil.fromJson(line, TaskJson.class);
                TaskStatusJson taskStatusJson = new TaskStatusJson(taskJson, "fail", LocalDate.now().toString());
                //System.out.println(taskStatusJson);
                taskStatusJsons.add(taskStatusJson);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try(FileWriter writer = new FileWriter(currentTaskFile, true)){
            String taskStatus=JsonUtil.toJson(taskStatusJsons);
            writer.write(taskStatus);
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return tasks;
    }

    @Override
    public void pushBackTask(String taskDetail) {
        insertToFile(currentTaskFile, taskDetail);
    }

    @Override
    public void logTaskError(String errorMessage) {
        insertToFile(logErrorFile, errorMessage);
    }

    @Override
    public void createTask(String task) {
        insertToFile(taskFile, task);
    }

    public void insertToFile(String file, String context) {
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.write(context + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
