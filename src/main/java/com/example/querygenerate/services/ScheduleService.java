package com.example.querygenerate.services;

import com.example.querygenerate.data.custom.TaskTime;
import com.example.querygenerate.solvers.task.FileStringTaskSolver;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class ScheduleService {

    private static final Logger LOGGER = Logger.getLogger(ScheduleService.class.getName());
    private final RunnerService runnerService;
    private final CompareService compareService;
    private final FileStringTaskSolver fileTaskSolver;
    private int successCount = 0;
    private final PriorityQueue<TaskTime> taskQueue = new PriorityQueue<>(
            Comparator.comparing(TaskTime::getExecuteTime).thenComparing(TaskTime::getDelayTimeMinutes)
    );
    @Scheduled(cron = "0 0 1 ? * MON", zone = "Asia/Ho_Chi_Minh")
    public void run() {
//        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
//        long startTime = System.currentTimeMillis();
//        for (final String fact : factTables) {
//            executor.execute(() -> {
////                try {
////                    runnerService.createQuery(fact, "dwh_test", LocalDate.now().toString(), "Dc2");
////                } catch (SQLException e) {
////                    throw new RuntimeException(e);
////                }
////                try {
////                    compareService.compareTemp1VsTemp3(fact, "dwh_test", LocalDate.now().toString(), "Dc2");
////                } catch (SQLException e) {
////                    throw new RuntimeException(e);
////                }
//            });
//        }
//        long endTime = System.currentTimeMillis();
//        LOGGER.log(Level.INFO, "total runtime ra3: {0}", endTime - startTime);
    }
    //@PostConstruct
    public void vc(){
        List<String> tasks=fileTaskSolver.readTask();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        for(String task:tasks){
            taskQueue.offer(new TaskTime(task,LocalDateTime.now(),0));
        }
        while(successCount<tasks.size()){
            if(!taskQueue.isEmpty()){
                TaskTime task = taskQueue.poll();
                if(LocalDateTime.now().isAfter(task.getExecuteTime())){
                    executor.execute(createRunnable(task,"dwh_test","2023-10-17"));
                    continue;
                }
                Duration duration = Duration.between(LocalDateTime.now(), task.getExecuteTime());
                scheduler.schedule(() -> executor.execute(createRunnable(task,"dwh_test","2023-10-17")), duration.getSeconds(), TimeUnit.SECONDS);
            }
        }
    }
    public Runnable createRunnable(TaskTime task, String schema, String day){
        return () -> {
            try {
                runnerService.createQuery(task.getTask(),
                        schema, day);
                fileTaskSolver.pushBackTask(task.getTask()+" done");
                successCount+=1;
            } catch (SQLException e) {
                System.out.println(e.getMessage());
                int currentDelay = task.getDelayTimeMinutes();
                taskQueue.offer(new TaskTime(task.getTask(), LocalDateTime.now().plusMinutes(currentDelay),(currentDelay==0?1:currentDelay*2)));
                fileTaskSolver.logTaskError(e.getMessage()+" "+LocalDateTime.now());
                fileTaskSolver.pushBackTask(task.getTask()+" fail");
                throw new RuntimeException(e);
            }
        };
    }
}
