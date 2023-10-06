package com.example.querygenerate.data;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author QuangNN
 */
@Data
public class EtlMap {
    private Map<String, String> dimFields;
    private List<String> dataFields;
}