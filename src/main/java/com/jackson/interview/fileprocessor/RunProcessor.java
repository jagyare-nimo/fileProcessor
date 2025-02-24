package com.jackson.interview.fileprocessor;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RunProcessor {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // Step 1: Initialize the downloader and download files
        S3Downloader downloader = new S3Downloader();
        downloader.downloadAndExtractAllCsv();

        Map<String, Map<String, List<BigDecimal>>> allFilesData = new HashMap<>();

        // Example: Parsing CSV files into the map (adjust the file paths as needed)
        allFilesData.put("MNZIRS0108", CsvProcessor.parseCsvFile("src/main/resources/csv/company-data/MNZIRS0108/MNZIRS0108.csv"));
        allFilesData.put("Y1HZ7B0146", CsvProcessor.parseCsvFile("src/main/resources/csv/company-data/Y1HZ7B0146/Y1HZ7B0146.csv"));
        allFilesData.put("U07N2S0124", CsvProcessor.parseCsvFile("src/main/resources/csv/company-data/U07N2S0124/U07N2S0124.csv"));
        allFilesData.put("Y8S4N80139", CsvProcessor.parseCsvFile("src/main/resources/csv/company-data/Y8S4N80139/Y8S4N80139.csv"));
        allFilesData.put("CT4OAR0154", CsvProcessor.parseCsvFile("src/main/resources/csv/company-data/CT4OAR0154/CT4OAR0154.csv"));

        // Example 1: Get value for "MO_BS_INV" on "2014-10-01" from MNZIRS0108
        BigDecimal valueForStartDate = CsvQuery.getValueForStartDate(allFilesData.get("MNZIRS0108"), "MO_BS_INV", "2014-10-01");
        System.out.println("Value for MO_BS_INV on 2014-10-01: " + valueForStartDate);

        // Example 2: Get mean of "MO_BS_AP" from Y1HZ7B0146
        BigDecimal meanForMO_BS_AP = CsvQuery.getMeanOfRow(allFilesData.get("Y1HZ7B0146"), "MO_BS_AP");
        System.out.println("Mean of MO_BS_AP in Y1HZ7B0146: " + meanForMO_BS_AP);

        // Example 3: Get value of "MO_BS_Intangibles" where date range includes "2015-09-30" from U07N2S0124
        BigDecimal valueForDateRange = CsvQuery.getValueForDateRange(allFilesData.get("U07N2S0124"), "MO_BS_Intangibles", "2015-09-30");
        System.out.println("Value for MO_BS_Intangibles on 2015-09-30: " + valueForDateRange);

        // Example 4: Get overall mean for "MO_BS_AR" across all files
        BigDecimal overallMeanMO_BS_AR = CsvQuery.getOverallMean(allFilesData, "MO_BS_AR");
        System.out.println("Overall mean of MO_BS_AR across all files: " + overallMeanMO_BS_AR);

        // Example 5: Get rows with values greater than 20,000,000,000 for "MO_BS_Goodwill"
        CsvQuery.getEntriesGreaterThanThreshold(allFilesData, "MO_BS_Goodwill", new BigDecimal("20000000000"));

        // Example 6: Get rows with 50% increase for "MO_BS_Goodwill" (increased values)
        CsvQuery.getRowsWith50PercentIncrease(allFilesData.get("Y8S4N80139"), "MO_BS_Goodwill");

        // Example 7: Get rows where values differ by 20% or more from the mean for "MO_BS_AR"
        CsvQuery.getRowsWithValuesDifferentFromMean(allFilesData.get("CT4OAR0154"), "MO_BS_AR");
    }

//    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
//        // Initialize the downloader
//        S3Downloader downloader = new S3Downloader();
//
//        // Download and extract all ZIP files from the S3 bucket
//        downloader.downloadAndExtractAllCsv();
//
//        //Initialize CsvProcessor
//        CsvProcessor processor = new CsvProcessor();
//
//        // processing a CSV file after it's extracted
//        String csvFilePath = "src/main/resources/csv/CT4OAR0154/CT4OAR0154.csv";  // Example file path
//        Map<String, List<BigDecimal>> dataMap = processor.parseCsvFile(csvFilePath);
//
//        // Example: Process CSV data
//        processor.processCsvData(dataMap);
//
//        // Get a specific value from a row and column
//        BigDecimal value = processor.getValueByRowAndColumn(dataMap, "MO_BS_INV", 1);  // Row: "MO_BS_INV", Column: 1
//        System.out.println("Value for MO_BS_INV, Column 1: " + value);
//
//        // Gracefully shutdown the executor service after the operation
//        downloader.shutdown();
//    }
}
