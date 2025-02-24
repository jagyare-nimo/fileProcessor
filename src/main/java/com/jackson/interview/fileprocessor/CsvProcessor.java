package com.jackson.interview.fileprocessor;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.math.BigDecimal;

public class CsvProcessor {

    /**
     * Parse a CSV file and return a map with row ID as the key and a list of BigDecimal values as the value.
     *
     * @param filePath the path of the CSV file
     * @return a map with row IDs as keys and list of BigDecimal values in each row
     * @throws IOException if an error occurs while reading the file
     */
    public Map<String, List<BigDecimal>> parseCsvFile(String filePath) throws IOException {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        for (int i = 1; i < lines.size(); i++) {
            String[] values = lines.get(i).split(",");
            String rowId = values[0];
            List<BigDecimal> dataValues = new ArrayList<>();
            for (int j = 1; j < values.length; j++) {
                try {
                    dataValues.add(new BigDecimal(values[j]));
                } catch (NumberFormatException e) {
                    dataValues.add(null);
                }
            }
            dataMap.put(rowId, dataValues);
        }

        return dataMap;
    }

    /**
     * Processing the CSV data and extracting specific values.
     * @param dataMap the map containing parsed data
     */
    public void processCsvData(Map<String, List<BigDecimal>> dataMap) {
        for (Map.Entry<String, List<BigDecimal>> entry : dataMap.entrySet()) {
            String rowId = entry.getKey();
            List<BigDecimal> values = entry.getValue();
            System.out.println("Row ID: " + rowId + ", First Value: " + (values.isEmpty() ? "No data" : values.get(0)));
        }
    }

    /**
     * Get specific value for a row and column (indexed by the column number).
     *
     * @param dataMap  the map containing parsed data
     * @param rowId    the row ID to look for
     * @param colIndex the column index to get the value from
     * @return the value from the specified row and column
     */
    public BigDecimal getValueByRowAndColumn(Map<String, List<BigDecimal>> dataMap, String rowId, int colIndex) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values != null && colIndex < values.size()) {
            return values.get(colIndex);
        }
        return null;
    }
}

