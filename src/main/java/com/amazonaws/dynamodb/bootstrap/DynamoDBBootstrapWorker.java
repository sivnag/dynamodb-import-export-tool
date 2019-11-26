/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * The base class to start a parallel scan and connect the results with a
 * consumer to accept the results.
 */
public class DynamoDBBootstrapWorker extends AbstractLogProvider {
    private final AmazonDynamoDBClient client;
    private final double rateLimit;
    private final String tableName;
    private final int numSegments;
    private int section;
    private int totalSections;
    private final boolean consistentScan;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBBootstrapWorker.class);

    /**
     * Creates the DynamoDBBootstrapWorker, calculates the number of segments a
     * table should have, and creates a thread pool to prepare to scan.
     * 
     * @throws Exception
     */
    public DynamoDBBootstrapWorker(AmazonDynamoDBClient client,
            double rateLimit, String tableName, ExecutorService exec,
            int section, int totalSections, int numSegments,
            boolean consistentScan) throws SectionOutOfRangeException {
        if (section > totalSections - 1 || section < 0) {
            throw new SectionOutOfRangeException(
                    "Section of scan must be within [0...totalSections-1]");
        }

        this.client = client;
        this.rateLimit = rateLimit;
        this.tableName = tableName;

        this.numSegments = numSegments;
        this.section = section;
        this.totalSections = totalSections;
        this.consistentScan = consistentScan;

        super.threadPool = exec;
    }

    /**
     * Creates the DynamoDBBootstrapWorker, calculates the number of segments a
     * table should have, and creates a thread pool to prepare to scan using an
     * eventually consistent scan.
     * 
     * @throws Exception
     */
    public DynamoDBBootstrapWorker(AmazonDynamoDBClient client,
            double rateLimit, String tableName, int numThreads)
            throws NullReadCapacityException {
        this.client = client;
        this.rateLimit = rateLimit;
        this.tableName = tableName;
        TableDescription description = client.describeTable(tableName)
                .getTable();
        this.section = 0;
        this.totalSections = 1;
        this.consistentScan = false;

        this.numSegments = getNumberOfSegments(description);
        int numProcessors = Runtime.getRuntime().availableProcessors() * 4;
        if (numProcessors > numThreads) {
            numThreads = numProcessors;
        }
        super.threadPool = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Begins to pipe the log results by parallel scanning the table and the
     * consumer writing the results.
     */
    public void pipe(final AbstractLogConsumer consumer)
            throws ExecutionException, InterruptedException, java.io.IOException {
        final DynamoDBTableScan scanner = new DynamoDBTableScan(rateLimit,
                client);

        final ScanRequest request = new ScanRequest().withTableName(tableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withLimit(BootstrapConstants.SCAN_LIMIT)
                .withConsistentRead(consistentScan);

        final ParallelScanExecutor scanService = scanner
                .getParallelScanCompletionService(request, numSegments,
                        threadPool, section, totalSections);

        Long scanned = 0L;
        while (!scanService.finished()) {
            SegmentedScanResult result = scanService.grab();
            scanned += result.getScanResult().getScannedCount();
            consumer.writeResult(result);
        }
        LOGGER.info("total scanned count = " + scanned);

        shutdown(true);
        consumer.shutdown(true);
    }

    /**
     * returns the approximate number of segments a table should be broken up
     * when parallel scanning. This function is based off of either read and
     * write capacity, with which you can scan much faster, or the size of your
     * table, which should need many more segments in order to scan the table
     * fast enough in parallel so that one worker does not finish long before
     * other workers.
     * 
     * @throws NullReadCapacityException
     *             if the table returns a null readCapacity units.
     */
    public static int getNumberOfSegments(TableDescription description)
            throws NullReadCapacityException {
        ProvisionedThroughputDescription provisionedThroughput = description
                .getProvisionedThroughput();
        double tableSizeInGigabytes = Math.ceil(description.getTableSizeBytes()
                / BootstrapConstants.GIGABYTE);
        Long readCapacity = provisionedThroughput.getReadCapacityUnits();
        Long writeCapacity = provisionedThroughput.getWriteCapacityUnits();

        if (readCapacity == null) {
            throw new NullReadCapacityException(
                    "Cannot scan with a null readCapacity provisioned throughput");
        }

        if(readCapacity.equals(0L))
            readCapacity = 500L;
        if(writeCapacity == null || writeCapacity.equals(0L))
            writeCapacity = 500L;

        double throughput = (readCapacity + 3 * writeCapacity) / 3000.0;
        return (int) (10 * Math.max(Math.ceil(throughput),
                Math.ceil(tableSizeInGigabytes) / 10));
    }

}
