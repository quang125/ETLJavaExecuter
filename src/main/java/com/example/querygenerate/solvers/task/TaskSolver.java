package com.example.querygenerate.solvers.task;

import java.util.List;

/**
 * @author QuangNN
 */
public interface TaskSolver {
    List<String> readTask();
    void pushBackTask(String taskDetail);
    void logTaskError(String errorMessage);
}
