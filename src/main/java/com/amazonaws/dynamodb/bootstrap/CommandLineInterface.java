/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.function.Function; 

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
/**
 * The interface that parses the arguments, and begins to transfer data from one
 * DynamoDB table to another
 */
public class CommandLineInterface {

    private static final Long SOURCE_RCU_DURING_REPLICA = 500L;
    private static final Long TARGET_WCU_DURING_REPLICA = 500L;
    private static final Long TARGET_LOW_WCU = 5L;
    private static final Long TARGET_LOW_RCU = 10L;

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(CommandLineInterface.class);

    private static void waitTillTableCreated(AmazonDynamoDBClient client, String tableName, CreateTableResult response) throws java.lang.InterruptedException
    {
        TableDescription tableDescription = response.getTableDescription();

        String status = tableDescription.getTableStatus();

        LOGGER.info(tableName + " - " + status);

        while (!status.equals("ACTIVE")){
            Thread.sleep(5000);
            try{
                DescribeTableResult res = client.describeTable(tableName);
                status = res.getTable().getTableStatus();
                LOGGER.info(tableName + " - " + status);
            }
            catch (ResourceNotFoundException e){}
        }
    }

    private static void waitTillTableUpdated(AmazonDynamoDBClient client, String tableName, UpdateTableResult response) throws java.lang.InterruptedException
    {
        TableDescription tableDescription = response.getTableDescription();

        String status = tableDescription.getTableStatus();

        LOGGER.info(tableName + " - " + status);
        while (!status.equals("ACTIVE")){
            Thread.sleep(5000);
            DescribeTableResult res = client.describeTable(tableName);
            status = res.getTable().getTableStatus();
            LOGGER.info(tableName + " - " + status);
        }
    }

    private static GlobalSecondaryIndex GlobalSecondaryIndexDescriptionToGlobalSecondaryIndex(GlobalSecondaryIndexDescription indexDescription){
        GlobalSecondaryIndex gsi = new GlobalSecondaryIndex()
            .withIndexName(indexDescription.getIndexName())
            .withKeySchema(indexDescription.getKeySchema())
            .withProjection(indexDescription.getProjection());

        Long readCapacity = indexDescription.getProvisionedThroughput().getReadCapacityUnits();
        if(!readCapacity.equals(TARGET_LOW_RCU))
            readCapacity = TARGET_LOW_RCU;
        Long writeCapacity = indexDescription.getProvisionedThroughput().getWriteCapacityUnits();
        if(!writeCapacity.equals(TARGET_LOW_WCU))
            writeCapacity = TARGET_LOW_WCU;
        gsi.setProvisionedThroughput(new ProvisionedThroughput());
        return gsi;
    }

    private static LocalSecondaryIndex LocalSecondaryIndexDescriptionToLocalSecondaryIndex(LocalSecondaryIndexDescription indexDescription){
        return new LocalSecondaryIndex()
            .withIndexName(indexDescription.getIndexName())
            .withKeySchema(indexDescription.getKeySchema())
            .withProjection(indexDescription.getProjection());
    }

    /**
     * Main class to begin transferring data from one DynamoDB table to another
     * DynamoDB table.
     * 
     * @param args
     */
    public static void main(String[] args) throws java.lang.InterruptedException, java.lang.Exception{
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);
        } catch (ParameterException e) {
            LOGGER.error(e);
            JCommander.getConsole().println(e.getMessage());
            cmd.usage();
            System.exit(1);
        }

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            return;
        }
        boolean resetTargetWCU = false;
        boolean resetSourceRCU = false;
        final String sourceEndpoint = params.getSourceEndpoint();
        final String destinationEndpoint = params.getDestinationEndpoint();
        final String destinationTable = params.getDestinationTable();

        LOGGER.info("destinationEndpoint = " + destinationEndpoint);
        boolean targetIsHardDisk = false;
        if(destinationEndpoint.equals("HardDisk")){
            LOGGER.info("dest is HardDisk");
            targetIsHardDisk = true;
        }
        LOGGER.info("targetIsHardDisk = " + targetIsHardDisk);

        final String sourceTable = params.getSourceTable();
        final double readThroughputRatio = params.getReadThroughputRatio();
        final double writeThroughputRatio = params.getWriteThroughputRatio();
        final int maxWriteThreads = params.getMaxWriteThreads();
        final boolean consistentScan = params.getConsistentScan();

        final ClientConfiguration sourceConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);
        final AmazonDynamoDBClient sourceClient = new AmazonDynamoDBClient(
                new DefaultAWSCredentialsProviderChain(), sourceConfig);
        sourceClient.setEndpoint(sourceEndpoint);

        TableDescription readTableDescription = sourceClient.describeTable(
                sourceTable).getTable();

        Long sourceReadCapacity = readTableDescription.getProvisionedThroughput().getReadCapacityUnits();
        Long sourceWriteCapacity = readTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
        if(sourceReadCapacity != null && sourceWriteCapacity != null)
        {
            if(sourceReadCapacity.equals(0L)){
                LOGGER.info("source table is in on-demand mode...no need to increase RCU");
            }
            else if(sourceReadCapacity.compareTo(SOURCE_RCU_DURING_REPLICA) < 0){
                LOGGER.info("source table read capacity = " + sourceReadCapacity + ". Needs update...");
                UpdateTableRequest request = new UpdateTableRequest()
                    .withTableName(sourceTable);
                request.setProvisionedThroughput(new ProvisionedThroughput(SOURCE_RCU_DURING_REPLICA, sourceWriteCapacity));
                UpdateTableResult response = sourceClient.updateTable(request);

                waitTillTableUpdated(sourceClient, sourceTable, response);

                DescribeTableResult res = sourceClient.describeTable(sourceTable);
                Long modifiedSourceReadCapacity = res.getTable().getProvisionedThroughput().getReadCapacityUnits();
                if(! SOURCE_RCU_DURING_REPLICA.equals(modifiedSourceReadCapacity))
                    throw new Exception("Could not set " + SOURCE_RCU_DURING_REPLICA + " RCUs for " + sourceTable + ". Current RCU="+modifiedSourceReadCapacity);
                resetSourceRCU = true;
            }
        }

        int numSegments = 10;
        try {
            numSegments = DynamoDBBootstrapWorker
                    .getNumberOfSegments(readTableDescription);
            LOGGER.info("numSegments = " + numSegments);
        } catch (NullReadCapacityException e) {
            LOGGER.warn("Number of segments not specified - defaulting to "
                    + numSegments, e);
        }
        final double readThroughput = calculateThroughput(readTableDescription,
                readThroughputRatio, true);

        AmazonDynamoDBClient destinationClient = null;
        TableDescription writeTableDescription = null;
        if(!targetIsHardDisk)
        {
            final ClientConfiguration destinationConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);
            destinationClient = new AmazonDynamoDBClient(
                    new DefaultAWSCredentialsProviderChain(), destinationConfig);
            destinationClient.setEndpoint(destinationEndpoint);

            try{
                writeTableDescription = destinationClient
                        .describeTable(destinationTable).getTable();

                boolean updateRequired = false;
                Long readCapacity = writeTableDescription.getProvisionedThroughput().getReadCapacityUnits();
                if(!readCapacity.equals(TARGET_LOW_RCU)){
                    LOGGER.info("target read capacity = " + readCapacity + ". Needs update");
                    readCapacity = TARGET_LOW_RCU;
                    updateRequired = true;
                }
                Long writeCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                if(writeCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0){
                    LOGGER.info("target write capacity = " + writeCapacity + ". Needs update");
                    writeCapacity = TARGET_WCU_DURING_REPLICA;
                    updateRequired = true;
                }

                if(updateRequired){
                    UpdateTableRequest request = new UpdateTableRequest()
                            .withTableName(destinationTable);
                    request.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
                    UpdateTableResult response = destinationClient.updateTable(request);

                    waitTillTableUpdated(destinationClient, destinationTable, response);

                    DescribeTableResult res = destinationClient.describeTable(destinationTable);
                    writeCapacity = res.getTable().getProvisionedThroughput().getWriteCapacityUnits();
                    if(!writeCapacity.equals(TARGET_WCU_DURING_REPLICA))
                        throw new Exception("Could not set " + TARGET_WCU_DURING_REPLICA + " WCUs for " + destinationTable + ". Current WCU="+writeCapacity);

                    resetTargetWCU = true;
                }
            }catch(ResourceNotFoundException e){
                if(params.shouldCreateDestinationTableIfNotFound()){
                    LOGGER.warn("destination table " + destinationTable + " not found. Creating using source table description...");
                    CreateTableRequest request = new CreateTableRequest()
                        .withAttributeDefinitions(readTableDescription.getAttributeDefinitions())
                        .withTableName(destinationTable)
                        .withKeySchema(readTableDescription.getKeySchema());

                    Long readCapacity = readTableDescription.getProvisionedThroughput().getReadCapacityUnits();
                    if(!readCapacity.equals(TARGET_LOW_RCU))
                        readCapacity = TARGET_LOW_RCU;
                    Long writeCapacity = readTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                    if(writeCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0)
                        writeCapacity = TARGET_WCU_DURING_REPLICA;
                    request.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));

                    java.util.Collection<GlobalSecondaryIndexDescription> gsid = readTableDescription.getGlobalSecondaryIndexes();
                    if(gsid != null){
                        java.util.List<GlobalSecondaryIndex> gsi = gsid.stream()
                            .map(index -> GlobalSecondaryIndexDescriptionToGlobalSecondaryIndex(index))
                            .collect(Collectors.toList());
                            request.setGlobalSecondaryIndexes(gsi);
                    }
                    java.util.Collection<LocalSecondaryIndexDescription> lsid = readTableDescription.getLocalSecondaryIndexes();
                    if(lsid != null){
                        java.util.List<LocalSecondaryIndex> lsi = lsid.stream()
                            .map(index -> LocalSecondaryIndexDescriptionToLocalSecondaryIndex(index))
                            .collect(Collectors.toList());
                        request.setLocalSecondaryIndexes(lsi);
                    }

                    CreateTableResult response = destinationClient.createTable(request);
                    waitTillTableCreated(destinationClient, destinationTable, response);
                    writeTableDescription = response.getTableDescription();
                    writeCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                    if(!writeCapacity.equals(TARGET_WCU_DURING_REPLICA))
                        throw new Exception("Could not set " + TARGET_WCU_DURING_REPLICA + " WCUs for " + destinationTable);
                    resetTargetWCU = true;
                }
                else
                    throw e;
            }

        }

        try {
            ExecutorService sourceExec = getSourceThreadPool(numSegments);
            ExecutorService destinationExec = getDestinationThreadPool(maxWriteThreads);

            AbstractLogConsumer consumer = null;
            if(targetIsHardDisk)
            {
                consumer = new DynamoDBConsumer2(destinationTable, destinationExec);
            }
            else{
                final double writeThroughput = calculateThroughput(
                        writeTableDescription, writeThroughputRatio, false);
                consumer = new DynamoDBConsumer(destinationClient,
                        destinationTable, writeThroughput, destinationExec);
            }

            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(
                    sourceClient, readThroughput, sourceTable, sourceExec,
                    params.getSection(), params.getTotalSections(), numSegments, consistentScan);

            LOGGER.info("Starting transfer...");
            worker.pipe(consumer);
            LOGGER.info("Finished Copying Table.");
        } catch (ExecutionException e) {
            LOGGER.error("Encountered exception when executing transfer.", e);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when executing transfer.", e);
            System.exit(1);
        } catch (SectionOutOfRangeException e) {
            LOGGER.error("Invalid section parameter", e);
        }

        if(resetSourceRCU == true)
        {
            UpdateTableRequest request = new UpdateTableRequest()
                .withTableName(sourceTable);
            request.setProvisionedThroughput(new ProvisionedThroughput(sourceReadCapacity, sourceWriteCapacity));

            try{
                UpdateTableResult response = sourceClient.updateTable(request);
                waitTillTableUpdated(sourceClient, sourceTable, response);
            }catch(Exception e){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                LOGGER.warn("Exception while resetting RCU, WCU for " + sourceTable + " " + e);
            }
        }

        {
            DescribeTableResult res = sourceClient.describeTable(sourceTable);
            readTableDescription = res.getTable();
            Long readCapacity = readTableDescription.getProvisionedThroughput().getReadCapacityUnits();
            Long writeCapacity = readTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
            if(!writeCapacity.equals(sourceWriteCapacity) || !readCapacity.equals(sourceReadCapacity)){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                String msg="Could not reset to " + sourceReadCapacity + " RCUs, " + sourceWriteCapacity + " WCUs for " + sourceTable;
                LOGGER.warn(msg);
                //throw new Exception(msg);
            }
        }

        if(resetTargetWCU == true)
        {
            UpdateTableRequest request = new UpdateTableRequest()
                .withTableName(destinationTable);
            request.setProvisionedThroughput(new ProvisionedThroughput(TARGET_LOW_RCU, TARGET_LOW_WCU));

            try{
                UpdateTableResult response = destinationClient.updateTable(request);
                waitTillTableUpdated(destinationClient, destinationTable, response);
            }catch(Exception e){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                LOGGER.warn("Exception while resetting lower RCU, WCU for " + destinationTable + " " + e);
            }
        }

        if(!targetIsHardDisk)
        {
            DescribeTableResult res = destinationClient.describeTable(destinationTable);
            writeTableDescription = res.getTable();
            Long readCapacity = writeTableDescription.getProvisionedThroughput().getReadCapacityUnits();
            Long writeCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
            if(!writeCapacity.equals(TARGET_LOW_WCU) || !readCapacity.equals(TARGET_LOW_RCU)){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                String msg="Could not reset to " + TARGET_LOW_RCU + " RCUs, " + TARGET_LOW_WCU + " WCUs for " + destinationTable;
                LOGGER.warn(msg);
                //throw new Exception(msg);
            }
        }
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     */
    private static double calculateThroughput(
            TableDescription tableDescription, double throughputRatio,
            boolean read) {
        double result = 0;
        if (read) {
            result = tableDescription.getProvisionedThroughput()
                    .getReadCapacityUnits() * throughputRatio;
        }
        else {
            result = tableDescription.getProvisionedThroughput()
                .getWriteCapacityUnits() * throughputRatio;
        }
        if(result == 0)
            result = 500 * throughputRatio;

        return result;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    private static ExecutorService getDestinationThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }

        LOGGER.info("destination thread pool corePoolSize = " + corePoolSize + " maxPoolSize = " + maxWriteThreads);

        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                maxWriteThreads, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(maxWriteThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

    /**
     * Returns the thread pool for the source DynamoDB table.
     */
    private static ExecutorService getSourceThreadPool(int numSegments) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > numSegments) {
            corePoolSize = numSegments - 1;
        }

        LOGGER.info("source thread pool corePoolSize = " + corePoolSize + " maxPoolSize = " + numSegments);

        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                numSegments, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(numSegments),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

}
