package com.jackson.interview.fileprocessor;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class S3Downloader {

    private static final String BUCKET_NAME = "as-findata-tech-challenge";
    private static final String REGION = "us-west-2";
    private final AmazonS3 s3Client;
    private final ExecutorService executorService;

    public S3Downloader() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .withCredentials(new ProfileCredentialsProvider())
                .build();

        this.executorService = Executors.newFixedThreadPool(4);
    }

    /**
     * Downloads and extracts all ZIP files from the company-data/ folder in the S3 bucket concurrently.
     * This method lists all the files in the folder and downloads each ZIP file concurrently.
     */
    public void downloadAndExtractAllCsv() throws IOException, InterruptedException, ExecutionException {
        // List all files in the S3 bucket under the company-data/ folder
        List<S3ObjectSummary> objectSummaries = listFilesInS3("company-data/");

        List<Callable<Void>> tasks = objectSummaries.stream()
                .filter(objectSummary -> objectSummary.getKey().endsWith(".zip"))
                .map(objectSummary -> (Callable<Void>) () -> {
                    downloadAndExtractCsv(objectSummary.getKey());
                    return null;  // Return null as the task result
                })
                .collect(Collectors.toList());

        List<Future<Void>> futures = executorService.invokeAll(tasks);

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    /**
     * Lists all files under the specified prefix (folder) in the S3 bucket.
     *
     * @param prefix the folder to list files from
     * @return a list of S3 object summaries
     */
    private List<S3ObjectSummary> listFilesInS3(String prefix) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(BUCKET_NAME).withPrefix(prefix);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        return result.getObjectSummaries();
    }

    /**
     * Downloads and extracts a specific ZIP file from the S3 bucket to the local filesystem.
     *
     * @param zipFileName the name of the ZIP file to download and extract
     * @throws IOException if an error occurs during the download or extraction process
     */
    public void downloadAndExtractCsv(String zipFileName) throws IOException {
        String downloadDir = "src/main/resources/csv";

        Path downloadPath = Paths.get(downloadDir);
        if (!Files.exists(downloadPath)) {
            Files.createDirectories(downloadPath);
        }

        String zipFilePath = downloadDir + File.separator + zipFileName;
        String extractDir = downloadDir + File.separator + zipFileName.replace(".zip", "");

        Path fullExtractDir = Paths.get(extractDir);
        if (!Files.exists(fullExtractDir)) {
            Files.createDirectories(fullExtractDir);
        }

        System.out.println("Downloading file from S3: " + zipFileName);

        // Download the ZIP file from S3
        S3Object s3Object = s3Client.getObject(BUCKET_NAME, zipFileName);
        try (InputStream inputStream = s3Object.getObjectContent();
             FileOutputStream fileOutputStream = new FileOutputStream(zipFilePath)) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, length);
            }
        }

        System.out.println("Extracting file to: " + extractDir);

        extractZipFile(zipFilePath, extractDir);
    }

    /**
     * Extracts the contents of a ZIP file to a specified directory.
     *
     * @param zipFilePath the path to the ZIP file
     * @param extractDir  the directory to extract the files into
     * @throws IOException if an error occurs during extraction
     */
    private void extractZipFile(String zipFilePath, String extractDir) throws IOException {
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                File extractedFile = new File(extractDir + File.separator + entry.getName());
                try (FileOutputStream fileOutputStream = new FileOutputStream(extractedFile)) {
                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = zipInputStream.read(buffer)) > 0) {
                        fileOutputStream.write(buffer, 0, length);
                    }
                }
            }
        }
    }

    // Shuts down the ExecutorService gracefully
    public void shutdown() {
        executorService.shutdown();
    }
}



