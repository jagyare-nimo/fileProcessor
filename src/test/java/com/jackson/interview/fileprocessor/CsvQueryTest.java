package com.jackson.interview.fileprocessor;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class CsvQueryTest {

    // 1. Return the value of the entry in row "MO_BS_INV" and with a start date of 2014-10-01 in file "MNZIRS0108.csv".
    @Test
    public void testGetValueForStartDate() {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        dataMap.put("MO_BS_INV", Arrays.asList(new BigDecimal("100"), new BigDecimal("200"), new BigDecimal("300")));

        BigDecimal result = CsvQuery.getValueForStartDate(dataMap, "MO_BS_INV", "2014-10-01");
        assertEquals(new BigDecimal("200"), result);
    }

    // 2. Return the mean of row "MO_BS_AP" in file "Y1HZ7B0146.csv".
    @Test
    public void testGetMeanOfRow() {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        dataMap.put("MO_BS_AP", Arrays.asList(new BigDecimal("100"), new BigDecimal("200"), new BigDecimal("300")));

        BigDecimal result = CsvQuery.getMeanOfRow(dataMap, "MO_BS_AP");
        assertEquals(new BigDecimal("200"), result);
    }

    // 3. Return the value of the entry in row "MO_BS_Intangibles" where the column's date range includes the date 2015-09-30 in file "U07N2S0124.csv".
    @Test
    public void testGetValueForDateRange() {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        dataMap.put("MO_BS_Intangibles", Arrays.asList(new BigDecimal("5000"), new BigDecimal("10000"), new BigDecimal("15000")));

        BigDecimal result = CsvQuery.getValueForDateRange(dataMap, "MO_BS_Intangibles", "2015-09-30");
        assertEquals(new BigDecimal("10000"), result);
    }

    // 4. Return a single number representing the mean of row "MO_BS_AR" over all input files.
    @Test
    public void testGetOverallMean() {
        Map<String, Map<String, List<BigDecimal>>> allFilesData = new HashMap<>();

        Map<String, List<BigDecimal>> fileData1 = new HashMap<>();
        fileData1.put("MO_BS_AR", Arrays.asList(new BigDecimal("100"), new BigDecimal("200")));

        Map<String, List<BigDecimal>> fileData2 = new HashMap<>();
        fileData2.put("MO_BS_AR", Arrays.asList(new BigDecimal("300"), new BigDecimal("400")));

        allFilesData.put("file1", fileData1);
        allFilesData.put("file2", fileData2);

        BigDecimal result = CsvQuery.getOverallMean(allFilesData, "MO_BS_AR");
        assertEquals(new BigDecimal("250"), result);
    }

    // 5. Return a single number representing the mean of row "MO_BS_NCI" over all input files.
    @Test
    public void testGetMeanOfMO_BS_NCI() {
        Map<String, Map<String, List<BigDecimal>>> allFilesData = new HashMap<>();

        Map<String, List<BigDecimal>> fileData1 = new HashMap<>();
        fileData1.put("MO_BS_NCI", Arrays.asList(new BigDecimal("50"), new BigDecimal("150")));

        Map<String, List<BigDecimal>> fileData2 = new HashMap<>();
        fileData2.put("MO_BS_NCI", Arrays.asList(new BigDecimal("250"), new BigDecimal("350")));

        allFilesData.put("file1", fileData1);
        allFilesData.put("file2", fileData2);

        BigDecimal result = CsvQuery.getMeanOfMO_BS_NCI(allFilesData, "MO_BS_NCI");
        assertEquals(new BigDecimal("200"), result);
    }

    // 6. Return the coordinates and values of all entries for row "MO_BS_Goodwill" in any file where the value is greater than 20,000,000,000.
    @Test
    public void testGetEntriesGreaterThanThreshold() {
        Map<String, Map<String, List<BigDecimal>>> allFilesData = new HashMap<>();
        Map<String, List<BigDecimal>> fileData1 = new HashMap<>();
        fileData1.put("MO_BS_Goodwill", Arrays.asList(new BigDecimal("10000000000"), new BigDecimal("25000000000")));
        allFilesData.put("file1", fileData1);
        assertDoesNotThrow(() -> CsvQuery.getEntriesGreaterThanThreshold(allFilesData, "MO_BS_Goodwill", new BigDecimal("20000000000")));
    }

    // 7. Return the IDs of rows where any of the values have increased by at least 50% from the first value over all time in file "Y8S4N80139.csv".
    @Test
    public void testGetRowsWith50PercentIncrease() {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        dataMap.put("MO_BS_Goodwill", Arrays.asList(new BigDecimal("100"), new BigDecimal("200"), new BigDecimal("160")));
        assertDoesNotThrow(() -> CsvQuery.getRowsWith50PercentIncrease(dataMap, "MO_BS_Goodwill"));
    }

    // 8. Return the IDs of rows where there is at least one value that differs by 20% or more from the mean of the row in file "CT4OAR0154.csv".
    @Test
    public void testGetRowsWithValuesDifferentFromMean() {
        Map<String, List<BigDecimal>> dataMap = new HashMap<>();
        dataMap.put("MO_BS_AR", Arrays.asList(new BigDecimal("100"), new BigDecimal("200"), new BigDecimal("400")));
        assertDoesNotThrow(() -> CsvQuery.getRowsWithValuesDifferentFromMean(dataMap, "MO_BS_AR"));
    }
}
