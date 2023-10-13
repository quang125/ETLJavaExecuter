package com.example.querygenerate.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author QuangNN
 */
@Component
@RequiredArgsConstructor
public class ScheduleService {
    private static final List<String> factTables = Arrays.asList("fact_uninstall", "fact_level_daily_time_play", "fact_inapp", "fact_property", "fact_level_difficulty",
            "fact_account_ads_view", "fact_session_time", "fact_resource_log", "fact_retention", "fact_ads_view", "fact_action", "fact_funnel");
    private static final Logger LOGGER = Logger.getLogger(ScheduleService.class.getName());
    private final RunnerService runnerService;
    private final CompareService compareService;

    @Scheduled(cron = "0 0 0 * * ?")
    public void run() {
        Thread[] factTablesThread = new Thread[factTables.size()];
        for (int i = 0; i < factTables.size(); i++) {
            final String fact = factTables.get(i);
            factTablesThread[i] = new Thread(() -> {
                try {
                    runnerService.createQuery(fact, "dwh_test", LocalDate.now().toString(), "DC2");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                try {
                    compareService.compareTemp(fact, "dwh_test", LocalDate.now().toString(), "DC2");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        long startTime = System.currentTimeMillis();
        for (Thread x : factTablesThread) {
            x.start();
        }
        try {
            for (Thread thread : factTablesThread) thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long endTime = System.currentTimeMillis();
        LOGGER.log(Level.INFO, "total runtime ra3: {0}", endTime - startTime);
    }
}
