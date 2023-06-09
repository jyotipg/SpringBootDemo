package com.lti.jasper.generator;

import java.util.HashMap;

/**
 * Hello world!
 */
public class ReportGeneratorFactory {

    private static final String BAL_KEY = "balKey";
    private static final String REPORT_PROP = "ReportProperties";
    private static final String JASPER_PATH = "JasperReportPath";

    private static String BAL_KEY_VALUE = "balance_sheet_detailed";
    private static String REPORT_PROP_VALUE = "BeanCollectionReportGeneratorClasses.properties";
    private static String JASPER_PATH_VALUE = "e:\\itc\\app\\eod\\moz\fcc\\ca\\BOReportrefresh\\common\\JasperReport";

    public static void main(String[] args) {
        setPropertyHeaders(args);
        System.out.println("BAL_KEY_VALUE : " + BAL_KEY_VALUE);
        System.out.println("REPORT_PROP_VALUE : " + REPORT_PROP_VALUE);
        System.out.println("JASPER_PATH_VALUE : " + JASPER_PATH_VALUE);
    }


    private static void setPropertyHeaders(String[] args) {
        HashMap<String, String> params = convertToKeyValuePair(args);
        if (params.containsKey(BAL_KEY)) {
            if ((params.get(BAL_KEY) != null) && params.get(BAL_KEY).length() != 0) {
                System.out.println("REPORT_PROP : " + params.get(BAL_KEY));
                BAL_KEY_VALUE = params.get(BAL_KEY);
            }
        }
        if (params.containsKey(JASPER_PATH)) {
            if ((params.get(JASPER_PATH) != null) && params.get(JASPER_PATH).length() != 0) {
                System.out.println("REPORT_PROP : " + params.get(JASPER_PATH));
                JASPER_PATH_VALUE = params.get(JASPER_PATH);
            }
        }

        if (params.containsKey(REPORT_PROP)) {
            if ((params.get(REPORT_PROP) != null) && params.get(REPORT_PROP).length() != 0) {

                System.out.println("REPORT_PROP : " + params.get(REPORT_PROP));
                REPORT_PROP_VALUE = params.get(REPORT_PROP);
            }
        }
    }

    private static HashMap<String, String> convertToKeyValuePair(String[] args) {

        HashMap<String, String> params = new HashMap<>();

        for (String arg : args) {

            String[] maps = arg.split(" ");
            for (String map : maps) {
                String[] splitFromEqual = map.split("=");
                String key = splitFromEqual[0];
                String value = splitFromEqual[1];
                params.put(key, value);
            }

        }

        return params;
    }
}
