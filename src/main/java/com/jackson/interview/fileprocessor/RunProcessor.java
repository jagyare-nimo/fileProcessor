package com.jackson.interview.fileprocessor;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RunProcessor {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // Initialize the downloader
        S3Downloader downloader = new S3Downloader();

        // Download and extract all ZIP files from the S3 bucket
        downloader.downloadAndExtractAllCsv();

        //Initialize CsvProcessor
        CsvProcessor processor = new CsvProcessor();

        // processing a CSV file after it's extracted
        String csvFilePath = "src/main/resources/csv/CT4OAR0154/CT4OAR0154.csv";  // Example file path
        Map<String, List<BigDecimal>> dataMap = processor.parseCsvFile(csvFilePath);

        // Example: Process CSV data
        processor.processCsvData(dataMap);

        // Get a specific value from a row and column
        BigDecimal value = processor.getValueByRowAndColumn(dataMap, "MO_BS_INV", 1);  // Row: "MO_BS_INV", Column: 1
        System.out.println("Value for MO_BS_INV, Column 1: " + value);

        // Gracefully shutdown the executor service after the operation
        downloader.shutdown();
    }
}
