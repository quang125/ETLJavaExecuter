package com.example.querygenerate.solvers.task;

import com.example.querygenerate.data.LogError;
import com.example.querygenerate.data.Task;
import com.example.querygenerate.data.TaskStatus;
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
public class FileStringTaskSolver implements TaskSolver {
    private static final String taskFile = "C:\\task\\task.txt";
    private static final String currentTaskFile = "C:\\task\\current.txt";
    private static final String logErrorFile = "C:\\task\\log.txt";

    @Override
    public List<Task> readTask() {
        List<Task> tasks = new ArrayList<>();
        try (FileReader fileReader = new FileReader(taskFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter writer = new FileWriter(currentTaskFile, true);) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(" ");
                tasks.add(new Task(split[0], split[1], LocalDate.now().toString()));
                writer.write(line + " " + LocalDate.now() + " " + " fail\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tasks;
    }

    @Override
    public void pushBackTask(TaskStatus taskStatus) {
        insertToFile(currentTaskFile, taskStatus.getTaskJson().getFactTable() + " " + taskStatus.getTaskJson().getSchema() + " " + taskStatus.getTaskJson().getDay() + " " + taskStatus.getStatus());
    }

    @Override
    public void logTaskError(LogError logError) {
        insertToFile(logErrorFile, logError.getContext());
    }


    @Override
    public List<TaskStatus> readUnDoneTask() {
        List<TaskStatus> taskStatuses = new ArrayList<>();
        try (FileReader fileReader = new FileReader(currentTaskFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader);) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(" ");
                taskStatuses.add(new TaskStatus(new Task(split[0], split[1], split[2]), split[3]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return taskStatuses;
    }

    public void insertToFile(String file, String context) {
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.write(context + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
