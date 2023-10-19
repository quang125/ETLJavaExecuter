package com.example.querygenerate.data.custom;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskTime {
    private String task;
    private LocalDateTime executeTime;
    private int delayTimeMinutes;
}
