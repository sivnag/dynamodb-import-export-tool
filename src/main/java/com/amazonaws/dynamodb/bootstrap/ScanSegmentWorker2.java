/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.Callable;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;


import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * This class executes multiple reads on one segment of a table's downloaded files in
 * series, as a runnable. Instances meant to be used as tasks of the worker
 * thread pool for parallel scans.
 * 
 */
public class ScanSegmentWorker2 implements Callable<SegmentedScanResult> {
    private String fileName;
    private int segment;
    private int startFileID;
    private int endFileID;
    private int scannedUptoFileID;

    private static final Logger LOGGER = LogManager
            .getLogger(ScanSegmentWorker2.class);

    ScanSegmentWorker2(String fileName, int startFileID, int endFileID, int segment) {
        this.fileName = fileName;
        this.startFileID = startFileID;
        this.endFileID = endFileID;
        this.segment = segment;
        scannedUptoFileID = startFileID - 1;
    }

    public boolean hasNext() {
        return scannedUptoFileID < endFileID;
    }

    @Override
    public SegmentedScanResult call() throws java.io.IOException{

        SegmentedScanResult result = new SegmentedScanResult(new ScanResult(), segment);

        if(startFileID <= 0 || endFileID <= 0){
            LOGGER.info("no files to scan for segment " + segment);
            return result;
        }

        ObjectMapper mapper = new ObjectMapper();

        mapper.addMixInAnnotations(AttributeValue.class,
                AttributeValueMixIn.class);

        int fileID = scannedUptoFileID +1;
        List<Map<String, AttributeValue>> scanResultItems =  new LinkedList<Map<String, AttributeValue>>();
        int count = 0;
        while (fileID <= endFileID) {
            java.io.File file = new java.io.File(fileName + "_" + fileID + ".json");
            if(file.exists()) {
                List<Map<String, AttributeValue>> batch =  new LinkedList<Map<String, AttributeValue>>();
                batch = mapper.readValue(file, new TypeReference<List<Map<String, AttributeValue>>>(){});
                scanResultItems.addAll(batch);
                scannedUptoFileID = fileID;
                count += batch.size();
                if(count >= BootstrapConstants.SCAN_LIMIT){
                    break;
                }
            }
            else
                scannedUptoFileID = fileID;
            fileID = scannedUptoFileID +1;
        }
        if(count > 0){
            ScanResult scanResult = new ScanResult().withItems(scanResultItems);
            result = new SegmentedScanResult(scanResult, segment);
        }
        LOGGER.info("scanned upto " + scannedUptoFileID + " files in segment "+ segment +" [" + startFileID +", "+ endFileID + "]");
        return result;
    }

}
