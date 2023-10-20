package com.example.querygenerate.data.json;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskStatusJson {
    private TaskJson taskJson;
    private String status;
    private String day;
}
