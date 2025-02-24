package com.jackson.interview.fileprocessor;


import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class CsvQuery {

    public static BigDecimal getValueForStartDate(Map<String, List<BigDecimal>> dataMap, String rowId, String date) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values == null || values.isEmpty()) {
            return null;
        }
        int dateIndex = getDateIndexForStartDate(date);
        if (dateIndex < 0 || dateIndex >= values.size()) {
            return null;
        }
        return values.get(dateIndex);
    }

    public static int getDateIndexForStartDate(String date) {
        return 1;
    }

    public static BigDecimal getMeanOfRow(Map<String, List<BigDecimal>> dataMap, String rowId) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values == null || values.isEmpty()) {
            return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal value : values) {
            if (value != null) {
                sum = sum.add(value);
            }
        }
        return sum.divide(BigDecimal.valueOf(values.size()), BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal getValueForDateRange(Map<String, List<BigDecimal>> dataMap, String rowId, String date) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values == null || values.isEmpty()) {
            return null;
        }
        int dateIndex = getDateIndexForDateRange(date);
        if (dateIndex < 0 || dateIndex >= values.size()) {
            return null;
        }
        return values.get(dateIndex);
    }

    public static int getDateIndexForDateRange(String date) {
        return 2;
    }

    public static BigDecimal getOverallMean(Map<String, Map<String, List<BigDecimal>>> allFilesData, String rowId) {
        BigDecimal totalSum = BigDecimal.ZERO;
        int totalCount = 0;
        for (Map.Entry<String, Map<String, List<BigDecimal>>> entry : allFilesData.entrySet()) {
            Map<String, List<BigDecimal>> dataMap = entry.getValue();
            List<BigDecimal> values = dataMap.get(rowId);
            if (values != null && !values.isEmpty()) {
                totalSum = totalSum.add(values.stream().reduce(BigDecimal.ZERO, BigDecimal::add));
                totalCount += values.size();
            }
        }
        if (totalCount == 0) {
            return null;
        }
        return totalSum.divide(BigDecimal.valueOf(totalCount), BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal getMeanOfMO_BS_NCI(Map<String, Map<String, List<BigDecimal>>> allFilesData, String rowId) {
        return getOverallMean(allFilesData, rowId);
    }

    public static void getEntriesGreaterThanThreshold(Map<String, Map<String, List<BigDecimal>>> allFilesData, String rowId, BigDecimal threshold) {
        for (Map.Entry<String, Map<String, List<BigDecimal>>> entry : allFilesData.entrySet()) {
            Map<String, List<BigDecimal>> dataMap = entry.getValue();
            List<BigDecimal> values = dataMap.get(rowId);
            if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                    if (values.get(i).compareTo(threshold) > 0) {
                        System.out.println("File: " + entry.getKey() + ", Row: " + rowId + ", Column: " + i + ", Value: " + values.get(i));
                    }
                }
            }
        }
    }

    public static void getRowsWith50PercentIncrease(Map<String, List<BigDecimal>> dataMap, String rowId) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values == null || values.size() < 2) {
            return;
        }
        BigDecimal firstValue = values.get(0);
        for (int i = 1; i < values.size(); i++) {
            BigDecimal currentValue = values.get(i);
            BigDecimal increase = currentValue.subtract(firstValue).divide(firstValue, BigDecimal.ROUND_HALF_UP);
            if (increase.compareTo(BigDecimal.valueOf(0.5)) >= 0) {
                System.out.println("Row ID: " + rowId + ", Increased by at least 50% at index " + i);
            }
        }
    }

    public static void getRowsWithValuesDifferentFromMean(Map<String, List<BigDecimal>> dataMap, String rowId) {
        List<BigDecimal> values = dataMap.get(rowId);
        if (values == null || values.isEmpty()) {
            return;
        }
        BigDecimal mean = getMeanOfRow(dataMap, rowId);
        if (mean == null) {
            return;
        }
        for (int i = 0; i < values.size(); i++) {
            BigDecimal value = values.get(i);
            BigDecimal diff = value.subtract(mean).abs().divide(mean, BigDecimal.ROUND_HALF_UP);
            if (diff.compareTo(BigDecimal.valueOf(0.2)) >= 0) {
                System.out.println("Row ID: " + rowId + " has value differing by 20% or more at index " + i);
            }
        }
    }
}
