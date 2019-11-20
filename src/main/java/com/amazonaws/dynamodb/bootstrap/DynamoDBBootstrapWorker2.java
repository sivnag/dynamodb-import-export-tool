/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
/**
 * The base class to start a parallel scan and connect the results with a
 * consumer to accept the results.
 */
public class DynamoDBBootstrapWorker2 extends AbstractLogProvider {
    private final String fileName;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker2.class);

    /**
     * Creates the DynamoDBBootstrapWorker2 that reads all files whose name
     * has pattern <fileName>_<XXXX>.json and 
     * @throws Exception
     */
    public DynamoDBBootstrapWorker2(String tableName, ExecutorService exec){
        this.fileName = tableName;
        super.threadPool = exec;
    }

    /**
     * Begins to pipe the log results by parallel scanning the table and the
     * consumer writing the results.
     */
    public void pipe(final AbstractLogConsumer consumer)
            throws ExecutionException, InterruptedException, java.io.IOException{

        ObjectMapper mapper = new ObjectMapper();

        mapper.addMixInAnnotations(AttributeValue.class,
                AttributeValueMixIn.class);

        int fileID = 1;
        java.io.File file = new java.io.File(this.fileName + "_" + fileID + ".json");
        boolean done = false;
        int count = 0;
        List<Map<String, AttributeValue>> scanResultItems =  new LinkedList<Map<String, AttributeValue>>();
        while (!done) {
            if(!file.exists())
                done = true;
            else{
                List<Map<String, AttributeValue>> batch =  new LinkedList<Map<String, AttributeValue>>();
                batch = mapper.readValue(file, new TypeReference<List<Map<String, AttributeValue>>>(){});
                scanResultItems.addAll(batch);
                count += batch.size();
                if(count >= BootstrapConstants.SCAN_LIMIT){
                    ScanResult scanResult = new ScanResult().withItems(scanResultItems);
                    SegmentedScanResult result = new SegmentedScanResult(scanResult, 1);//hard coded segment to #1 which is anyway not used in this code flow
                    consumer.writeResult(result);
                    LOGGER.info("scanned upto " + fileID + " files and written " + count + " more items to consumer");
                    count = 0;
                    scanResultItems =  new LinkedList<Map<String, AttributeValue>>();
                }
                ++fileID;
                file = new java.io.File(this.fileName + "_" + fileID + ".json");
            }
        }
        if(count > 0){
            ScanResult scanResult = new ScanResult().withItems(scanResultItems);
            SegmentedScanResult result = new SegmentedScanResult(scanResult, 1);//hard coded segment to #1 which is anyway not used in this code flow
            consumer.writeResult(result);
        }
        LOGGER.info("total scanned files = " + (fileID-1));

        shutdown(true);
        consumer.shutdown(true);
    }
}
