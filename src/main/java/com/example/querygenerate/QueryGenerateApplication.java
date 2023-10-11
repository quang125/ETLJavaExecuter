package com.example.querygenerate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class QueryGenerateApplication {
    public static void main(String[] args) {

        SpringApplication.run(QueryGenerateApplication.class, args);
    }
}
