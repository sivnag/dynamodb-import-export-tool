/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Callable class that is used to write a batch of items to DynamoDB with exponential backoff.
 */
public class DynamoDBConsumerWorker2 implements Callable<Void> {

    private List<Map<String, AttributeValue>> batch;
    private final String fileName;
    private static int ___ID = 0;
    private int __ID;
    private static Object synchro = new Object();

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker2.class);

    /**
     * Callable class that when called will try to write a batch to a file <filename><__ID>.json
        in current working dir
     */
    public DynamoDBConsumerWorker2(List<Map<String, AttributeValue>> batch, String fileName) {
        synchronized(synchro){
            this.__ID = ++DynamoDBConsumerWorker2.___ID;
        }
        this.batch = batch;
        this.fileName = fileName;
    }

    /**
     * writes the batch of items to a file <filename><__ID>.json
        in current working dir
     */
    @Override
    public Void call() throws JsonProcessingException, java.io.IOException{
        ObjectMapper mapper = new ObjectMapper();

        mapper.addMixInAnnotations(AttributeValue.class,
                AttributeValueMixIn.class);

        mapper.writeValue(new java.io.File(fileName + "_" + __ID + ".json"), batch);
        return null;
    }

}
