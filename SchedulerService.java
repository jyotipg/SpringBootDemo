package com.com.scheduled.service;

import com.com.service.ReportGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class SchedulerService {
    @Autowired
    ReportGenerator reportProcedure;
    private ScheduledExecutorService scheduledExecutorService = null;

    @PostConstruct
    public void init() {
        startService();
    }

    private void startService() {
        System.out.println("Starting Service................");
        if (scheduledExecutorService == null) {

            scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
            scheduledExecutorService.schedule(new Runnable() {
                public void run() {
                    processData();
                }
            }, 1, TimeUnit.MINUTES);
        }
    }

    private void closeScheduledExecutorService() {
        System.out.println("Closing Service................");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            // (Re-)Cancel if current thread also interrupted
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

    }

    private void processData() {
        closeScheduledExecutorService();
        reportProcedure.startReport();
        startService();

    }
}
