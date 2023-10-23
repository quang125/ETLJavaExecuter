package com.example.querygenerate.services;

import com.example.querygenerate.data.LogError;
import com.example.querygenerate.data.Task;
import com.example.querygenerate.data.TaskStatus;
import com.example.querygenerate.data.custom.TaskTime;
import com.example.querygenerate.faction.variances.DelayAction;
import com.example.querygenerate.factory.TaskFactory;
import com.example.querygenerate.solvers.task.TaskSolver;
import com.example.querygenerate.utils.QueryGenerateUtils;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class ScheduleService implements InitializingBean {

    private final RunnerService runnerService;
    private final CompareService compareService;
    private final TaskFactory taskFactory;
    private final PriorityQueue<TaskTime> taskQueue = new PriorityQueue<>(
            Comparator.comparing(TaskTime::getExecuteTime).thenComparing(TaskTime::getDelayTimeMinutes)
    );
    Set<String> doneTask = new HashSet<>();
    private TaskSolver taskSolver;
    private int successCount = 0;
    Scanner sc=new Scanner(System.in);
    @PostConstruct
    public void initTaskSolver() {
        taskSolver=taskFactory.createTask(sc.next());
    }

    @Scheduled(cron = "0 0 1 * * ?", zone = "Asia/Ho_Chi_Minh")
    public void dailyRun() {
        List<Task> tasks = taskSolver.readTask();
        doTask(tasks);
    }

    public Runnable createRunnable(TaskTime taskTime, String day, TaskSolver taskSolver) {
        return () -> {
            try {
                runnerService.createQuery(taskTime.getTask(),
                        taskTime.getSchema(), day);
                taskSolver.pushBackTask(new TaskStatus(new Task(taskTime.getTask().trim(), taskTime.getSchema().trim(), day.trim()), "done"));
                successCount += 1;
            } catch (SQLException e) {
                int currentDelay = taskTime.getDelayTimeMinutes();
                taskQueue.offer(new TaskTime(taskTime.getTask(), taskTime.getSchema(), day, LocalDateTime.now().plusMinutes(currentDelay), (currentDelay == 0 ? 1 : currentDelay * 2)));
                taskSolver.logTaskError(new LogError(e.getMessage()+" "+taskTime.getSchema()+" "+LocalDateTime.now()));
                taskSolver.pushBackTask(new TaskStatus(new Task(taskTime.getTask().trim(), taskTime.getSchema().trim(), day.trim()), "fail"));
                throw new RuntimeException(e);
            }
        };
    }

    public void doTask(List<Task> tasks){
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        for (Task task : tasks) {
            System.out.println(task.toString());
            taskQueue.offer(new TaskTime(task.getFactTable(), task.getSchema(), task.getDay(), LocalDateTime.now(), 0));
        }
        while (successCount < tasks.size()) {
            if (!taskQueue.isEmpty()) {
                TaskTime task = taskQueue.poll();
                if (LocalDateTime.now().isAfter(task.getExecuteTime())) {
                    executor.execute(createRunnable(task, task.getDay(), taskSolver));
                    continue;
                }
                Duration duration = Duration.between(LocalDateTime.now(), task.getExecuteTime());
                DelayAction delayAction = new DelayAction(createRunnable(task,task.getDay(), taskSolver),duration);
                delayAction.schedule();
            }
        }
    }


    @Override
    public void afterPropertiesSet() throws SQLException {
        List<TaskStatus> taskStatuses = taskSolver.readUnDoneTask();
        List<Task> undoneTasks = new ArrayList<>();
        for (int i = taskStatuses.size() - 1; i >= 0; i--) {
            String task = taskStatuses.get(i).getTaskJson().getSchema() + " " + taskStatuses.get(i).getTaskJson().getFactTable() + " " + taskStatuses.get(i).getTaskJson().getDay();
            if (taskStatuses.get(i).getStatus().equals("done")) {
                doneTask.add(task);
            } else {
                if (!doneTask.contains(task)) {
                    undoneTasks.add(new Task(taskStatuses.get(i).getTaskJson().getFactTable(), taskStatuses.get(i).getTaskJson().getSchema(),
                            taskStatuses.get(i).getTaskJson().getDay()));
                    doneTask.add(task);
                }
            }
        }
        doTask(undoneTasks);
    }
}
