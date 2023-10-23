package com.example.querygenerate.solvers.task;

import com.example.querygenerate.data.LogError;
import com.example.querygenerate.data.Table;
import com.example.querygenerate.data.Task;
import com.example.querygenerate.data.TaskStatus;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.*;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class FileJsonTaskSolver implements TaskSolver {
    private static final String taskFile = "C:\\task\\task.json";
    private static final String currentTaskFile = "C:\\task\\current.json";
    private static final String logErrorFile = "C:\\task\\log.json";
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public List<Task> readTask() {
        List<Task>tasks=new ArrayList<>();
        List<Table>tables=new ArrayList<>();
        try (Reader reader = new FileReader(taskFile);
        ) {
            Type listType = new TypeToken<List<Table>>() {}.getType();
            tables = gson.fromJson(reader,listType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<TaskStatus> oldTask=readOldTask();
        if(oldTask==null) oldTask=new ArrayList<>();
        for(Table t : tables){
            Task task=new Task(t.getFactTable().trim(),t.getSchema().trim(),LocalDate.now().minusDays(2).toString().trim());
            tasks.add(task);
            oldTask.add(new TaskStatus(task,"fail"));
        }
        try (Writer writer = new FileWriter(currentTaskFile)) {
            gson.toJson(oldTask, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tasks;
    }

    private List<TaskStatus> readOldTask() {
        List<TaskStatus>oldTask=new ArrayList<>();
        try (Reader reader = new FileReader(currentTaskFile)) {
            Type listType = new TypeToken<List<TaskStatus>>() {}.getType();
            oldTask = gson.fromJson(reader, listType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return oldTask;
    }

    @Override
    public synchronized void pushBackTask(TaskStatus taskStatus) {
        List<TaskStatus>oldTask=readOldTask();
        if(oldTask == null) oldTask=new ArrayList<>();
        oldTask.add(taskStatus);
        try (Writer writer = new FileWriter(currentTaskFile)) {
            gson.toJson(oldTask, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void logTaskError(LogError logError) {
        List<LogError> logErrors = new ArrayList<>();
        try (Reader reader = new FileReader(logErrorFile)) {
            Type listType = new TypeToken<List<LogError>>() {}.getType();
            logErrors = gson.fromJson(reader, listType);
            if (logErrors == null) {
                logErrors = new ArrayList<>();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logErrors.add(logError);
        try (Writer writer = new FileWriter(logErrorFile)) {
            gson.toJson(logErrors, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TaskStatus> readUnDoneTask() {
        List<TaskStatus> taskStatuses = new ArrayList<>();
        try (Reader reader = new FileReader(currentTaskFile)) {
            Type listType = new TypeToken<List<TaskStatus>>() {}.getType();
            taskStatuses = gson.fromJson(reader, listType);
            if(taskStatuses==null) taskStatuses=new ArrayList<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return taskStatuses;
    }
}
