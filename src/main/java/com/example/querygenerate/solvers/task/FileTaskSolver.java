package com.example.querygenerate.solvers.task;

import com.example.querygenerate.services.CompareService;
import com.example.querygenerate.services.RunnerService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class FileTaskSolver implements TaskSolver{
    private static String taskFile="C:\\task\\task.txt";
    private String currentTaskFile="C:\\task\\current.txt";
    private String logErrorFile="C:\\task\\log.txt";
    @Override
    public List<String> readTask(){
        List<String> tasks=new ArrayList<>();
        try (FileReader fileReader = new FileReader(taskFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter writer = new FileWriter(currentTaskFile, true);) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                tasks.add(line);
                writer.write(line+" fail\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tasks;
    }

    @Override
    public void pushBackTask(String taskDetail) {
        try(FileWriter writer = new FileWriter(currentTaskFile, true);){
            writer.write(taskDetail+"\n");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void logTaskError(String errorMessage){
        try(FileWriter writer = new FileWriter(logErrorFile, true);){
            writer.write(errorMessage+"\n");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}
