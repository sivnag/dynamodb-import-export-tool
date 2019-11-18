/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Takes in SegmentedScanResults and launches several DynamoDBConsumerWorker for
 * each batch of items to write to a DynamoDB table.
 */
public class DynamoDBConsumer2 extends AbstractLogConsumer {

    private final String fileName;
    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumer2.class);
    /**
     * Class to consume logs and write them to multiple local file(s) on HardDisk
     * with <filename>UID.json name pattern in current working dir
     */
    public DynamoDBConsumer2(String tableName, ExecutorService exec) {
        fileName = tableName;
        super.threadPool = exec;
        super.exec = new ExecutorCompletionService<Void>(threadPool);
    }

    @Override
    public Future<Void> writeResult(SegmentedScanResult result) {
        Future<Void> jobSubmission = null;
        List<List<Map<String, AttributeValue>>> batches = splitResultIntoBatches(
                result.getScanResult(), fileName);
        Iterator<List<Map<String, AttributeValue>>> batchesIterator = batches.iterator();
        while (batchesIterator.hasNext()) {
            try {
                jobSubmission = exec
                        .submit(new DynamoDBConsumerWorker2(batchesIterator.next(), fileName));
            } catch (NullPointerException npe) {
                throw new NullPointerException(
                        "Thread pool not initialized for LogStashExecutor");
            }
        }
        return jobSubmission;
    }

    /**
     * Splits up a ScanResult into a list of BatchWriteItemRequests of size 25
     * items or less each.
     */
    public static List<List<Map<String, AttributeValue>>> splitResultIntoBatches(
            ScanResult result, String fileName) {
        List<List<Map<String, AttributeValue>>> batches = new LinkedList<List<Map<String, AttributeValue>>>();
        Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();

        int i = 0;
        long j = 0L;
        int k = 0;
        List<Map<String, AttributeValue>> batch = new LinkedList<Map<String, AttributeValue>>();
        while (it.hasNext()) {
            
            batch.add(it.next());

            i++;
            ++j;
            if (i == BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM) {
                batches.add(batch);
                ++k;
                batch = new LinkedList<Map<String, AttributeValue>>();
                i = 0;
            }
        }
        if (i > 0) {
            batches.add(batch);
            ++k;
        }
        LOGGER.info("split " + j + " records into " + k + " batches");
        return batches;
    }
}
