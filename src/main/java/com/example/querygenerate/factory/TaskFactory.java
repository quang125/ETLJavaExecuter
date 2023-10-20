package com.example.querygenerate.factory;

import com.example.querygenerate.solvers.task.FileJsonTaskSolver;
import com.example.querygenerate.solvers.task.FileStringTaskSolver;
import com.example.querygenerate.solvers.task.TaskSolver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class TaskFactory {
    private final FileJsonTaskSolver fileJsonTaskSolver;
    private final FileStringTaskSolver fileStringTaskSolver;
    public TaskSolver createTask(String taskKind) {
        return switch (taskKind) {
            case "FileString" -> fileStringTaskSolver;
            case "FileJson" -> fileJsonTaskSolver;
            default -> throw new RuntimeException("not support kind of resources");
        };
    }
}
