package com.example.querygenerate.data;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class Task {
    private String factTable;
    private String schema;
    private String day;
}
