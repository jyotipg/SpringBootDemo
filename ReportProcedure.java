package com.com.service;

import org.springframework.stereotype.Component;

@Component
public class ReportProcedure implements IReportProcedure {


    @Override
    public void generateReport(String output, String codeBank, String bankName) {

        System.out.println("Generating Report ");

        for (int i = 0; i < 10; i++) {
            System.out.println("Counting " + i);

        }


    }

}
