/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

/**
 * Callable class that is used to write a batch of items to DynamoDB with exponential backoff.
 */
public class DynamoDBConsumerWorker implements Callable<Void> {

    private final AmazonDynamoDBClient client;
    private final RateLimiter rateLimiter;
    private long exponentialBackoffTime;
    private BatchWriteItemRequest batch;
    private final String tableName;
    private static int ___ID = 0;
    private int __ID;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker.class);

    /**
     * Callable class that when called will try to write a batch to a DynamoDB
     * table. If the write returns unprocessed items it will exponentially back
     * off until it succeeds.
     */
    public DynamoDBConsumerWorker(BatchWriteItemRequest batchWriteItemRequest,
            AmazonDynamoDBClient client, RateLimiter rateLimiter,
            String tableName) {
        this.__ID = ++DynamoDBConsumerWorker.___ID;
        this.batch = batchWriteItemRequest;
        this.client = client;
        this.rateLimiter = rateLimiter;
        this.tableName = tableName;
        this.exponentialBackoffTime = BootstrapConstants.INITIAL_RETRY_TIME_MILLISECONDS;
    }

    /**
     * Batch writes the write request to the DynamoDB endpoint and THEN acquires
     * permits equal to the consumed capacity of the write.
     */
    @Override
    public Void call() {
        LOGGER.info("Consumer" + __ID + " about to batch write " + batch.getRequestItems().get(tableName).size() + " records");
        //List<ConsumedCapacity> batchResult = 
        runWithBackoff(batch);
        LOGGER.info("Consumer" + __ID + " finished writing");
        /*Iterator<ConsumedCapacity> it = batchResult.iterator();
        int consumedCapacity = 0;
        while (it.hasNext()) {
            consumedCapacity += it.next().getCapacityUnits().intValue();
        }
        rateLimiter.acquire(consumedCapacity);*/
        return null;
    }

    /**
     * Writes to DynamoDBTable using an exponential backoff. If the
     * batchWriteItem returns unprocessed items then it will exponentially
     * backoff and retry the unprocessed items.
     */
    public void 
    //List<ConsumedCapacity> 
    runWithBackoff(BatchWriteItemRequest req) {
        BatchWriteItemResult writeItemResult = null;
        //List<ConsumedCapacity> consumedCapacities = new LinkedList<ConsumedCapacity>();
        Map<String, List<WriteRequest>> unprocessedItems = null;
        boolean interrupted = false;
        boolean throughputExceeded = false;
        try {
            do {
                throughputExceeded = false;
                writeItemResult = null;
                unprocessedItems = null;
                try{
                    writeItemResult = client.batchWriteItem(req);
                }catch(ProvisionedThroughputExceededException e){
                    LOGGER.warn("ProvisionedThroughputExceededException @ consumer " + __ID);
                    throughputExceeded = true;
                }

                if(writeItemResult != null){
                    unprocessedItems = writeItemResult.getUnprocessedItems();
                    //consumedCapacities.addAll(writeItemResult.getConsumedCapacity());
                    List<ConsumedCapacity> batchResult = writeItemResult.getConsumedCapacity();
                    Iterator<ConsumedCapacity> it = batchResult.iterator();
                    int consumedCapacity = 0;
                    while (it.hasNext()) {
                        consumedCapacity += it.next().getCapacityUnits().intValue();
                    }
                    rateLimiter.acquire(consumedCapacity);
                }

                if(unprocessedItems != null || throughputExceeded){
                    if (unprocessedItems != null)
                        req.setRequestItems(unprocessedItems);
                    try {
                        LOGGER.info("consumer " + __ID + " going to sleep for " + exponentialBackoffTime + "mS");
                        Thread.sleep(exponentialBackoffTime);
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime *= 2;
                        if (exponentialBackoffTime > BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME) {
                            exponentialBackoffTime = BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME;
                        }
                    }
                }
            } while (throughputExceeded || (unprocessedItems != null && unprocessedItems.get(tableName) != null));
            //return consumedCapacities;
        }catch(Exception e){
            LOGGER.warn("Consumer " + __ID + "died facing exception " + e);
            throw e;
        } 
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
