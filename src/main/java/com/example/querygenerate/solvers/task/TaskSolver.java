package com.example.querygenerate.solvers.task;

import com.example.querygenerate.data.LogError;
import com.example.querygenerate.data.Task;
import com.example.querygenerate.data.TaskStatus;

import java.util.List;

/**
 * @author QuangNN
 */
public interface TaskSolver {
    List<Task> readTask();
    void pushBackTask(TaskStatus taskStatus);
    void logTaskError(LogError logError);
    List<TaskStatus>readUnDoneTask();
}
