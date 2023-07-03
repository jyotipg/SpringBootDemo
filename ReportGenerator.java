package com.com.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ReportGenerator {
    @Autowired
    IReportProcedure reportProcedure;

    public void startReport()
    {
        reportProcedure.generateReport("test1", "test1", "test1");
    }

}
