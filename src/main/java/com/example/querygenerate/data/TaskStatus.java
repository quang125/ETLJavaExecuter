package com.example.querygenerate.data;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskStatus {
    private Task taskJson;
    private String status;
}
