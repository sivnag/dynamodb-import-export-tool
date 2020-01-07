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

/**
 * The base class to start a parallel scan and connect the results with a
 * consumer to accept the results.
 */
public class DynamoDBBootstrapWorker2 extends AbstractLogProvider {
    private final String fileName;
    private final int numSegments; 
    private final int maxFileID;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker2.class);

    /**
     * Creates the DynamoDBBootstrapWorker2 that reads all files whose name
     * has pattern <fileName>_<XXXX>.json and 
     * @throws Exception
     */
    public DynamoDBBootstrapWorker2(int maxFileID, int numSegments, String tableName, ExecutorService exec){
        this.maxFileID = maxFileID;
        this.numSegments = numSegments;
        this.fileName = tableName;
        super.threadPool = exec;
    }

    /**
     * Begins to pipe the log results by parallel scanning the table and the
     * consumer writing the results.
     */
    public void pipe(final AbstractLogConsumer consumer)
            throws ExecutionException, InterruptedException, java.io.IOException{

        final DynamoDBFileScan scanner = new DynamoDBFileScan();

        final ParallelScanExecutor2 scanService = scanner
                .getParallelScanCompletionService(fileName, maxFileID, numSegments, threadPool);

        while (!scanService.finished()) {
            SegmentedScanResult result = scanService.grab();
            consumer.writeResult(result);
        }

        shutdown(true);
        consumer.shutdown(true);
    }
}
