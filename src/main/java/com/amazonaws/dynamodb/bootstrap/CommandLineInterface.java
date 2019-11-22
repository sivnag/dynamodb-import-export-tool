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
import com.amazonaws.dynamodb.bootstrap.exception.NullCapacityException;
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
        if(readCapacity.equals(0L))
            readCapacity = TARGET_LOW_RCU;
        Long writeCapacity = indexDescription.getProvisionedThroughput().getWriteCapacityUnits();
        if(writeCapacity.equals(0L))
            writeCapacity = TARGET_LOW_WCU;
        gsi.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
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
        LOGGER.info("sourceEndpoint = " + sourceEndpoint);
        boolean sourceIsHardDisk = false;
        if(sourceEndpoint.equals("HardDisk")){
            LOGGER.info("source is HardDisk");
            sourceIsHardDisk = true;
        }
        LOGGER.info("sourceIsHardDisk = " + sourceIsHardDisk);

        final String destinationEndpoint = params.getDestinationEndpoint();
        LOGGER.info("destinationEndpoint = " + destinationEndpoint);
        boolean targetIsHardDisk = false;
        if(destinationEndpoint.equals("HardDisk")){
            LOGGER.info("dest is HardDisk");
            targetIsHardDisk = true;
        }
        LOGGER.info("targetIsHardDisk = " + targetIsHardDisk);

        if(sourceIsHardDisk && targetIsHardDisk)
        {
            LOGGER.info("both source and destination cannot be HardDisk!");
            System.exit(1);
        }

        if(sourceIsHardDisk && params.shouldCreateDestinationTableIfNotFound())
        {
            //creation of taretTable requires some knowledge about source table
            //which we don't have at present when source is HardDisk
            LOGGER.info("createDestinationTableIfNotFound flag is not supported when source is HardDisk!");
            System.exit(1);
        }

        if(sourceIsHardDisk && params.getMaxFileID() == 0)
        {
            LOGGER.info("please provide --max-file-id when source is HardDisk!");
            System.exit(1);
        }

        final String destinationTable = params.getDestinationTable();
        final String sourceTable = params.getSourceTable();
        final double readThroughputRatio = params.getReadThroughputRatio();
        final double writeThroughputRatio = params.getWriteThroughputRatio();
        final int maxWriteThreads = params.getMaxWriteThreads();
        final boolean consistentScan = params.getConsistentScan();

        TableDescription readTableDescription = null;
        AmazonDynamoDBClient sourceClient = null;
        int numSegments = 10;
        Long sourceReadCapacity = 0L;
        Long sourceWriteCapacity = 0L;
        Long targetReadCapacity = 0L;
        Long targetWriteCapacity = 0L;

        if(!sourceIsHardDisk)
        {
            final ClientConfiguration sourceConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);
            sourceClient = new AmazonDynamoDBClient(
                    new DefaultAWSCredentialsProviderChain(), sourceConfig);
            sourceClient.setEndpoint(sourceEndpoint);

            readTableDescription = sourceClient.describeTable(
                    sourceTable).getTable();

            sourceReadCapacity = readTableDescription.getProvisionedThroughput().getReadCapacityUnits();
            sourceWriteCapacity = readTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
            if(sourceReadCapacity == null || sourceWriteCapacity == null)
                throw new NullCapacityException("Source table " + sourceTable + " has null capacity provisioning");

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

            try {
                numSegments = DynamoDBBootstrapWorker
                        .getNumberOfSegments(readTableDescription);
                LOGGER.info("numSegments = " + numSegments);
            } catch (NullReadCapacityException e) {
                LOGGER.warn("Number of segments not specified - defaulting to "
                        + numSegments, e);
            }
        }

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
                targetReadCapacity = writeTableDescription.getProvisionedThroughput().getReadCapacityUnits();
                targetWriteCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                if(targetReadCapacity == null || targetWriteCapacity == null)
                    throw new NullCapacityException("Target table " + destinationTable + " has null capacity provisioning");

                if(targetWriteCapacity.equals(0L)){
                    LOGGER.info("target table is in on-demand mode...no need to increase WCU");
                }
                else if(targetWriteCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0){
                    LOGGER.info("target write capacity = " + targetWriteCapacity + ". Needs update");
                    targetWriteCapacity = TARGET_WCU_DURING_REPLICA;
                    updateRequired = true;
                }

                if(updateRequired){
                    UpdateTableRequest request = new UpdateTableRequest()
                            .withTableName(destinationTable);
                    request.setProvisionedThroughput(new ProvisionedThroughput(targetReadCapacity, targetWriteCapacity));
                    UpdateTableResult response = destinationClient.updateTable(request);

                    waitTillTableUpdated(destinationClient, destinationTable, response);

                    DescribeTableResult res = destinationClient.describeTable(destinationTable);
                    writeTableDescription = res.getTable();
                    Long writeCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                    if(writeCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0)
                        throw new Exception("Could not set at least " + TARGET_WCU_DURING_REPLICA + " WCUs for " + destinationTable + ". Current WCU="+writeCapacity);

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
                    if(readCapacity.equals(0L))
                        targetReadCapacity = TARGET_LOW_RCU;
                    else
                        targetReadCapacity = readCapacity;
                    Long writeCapacity = readTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
                    if(writeCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0){
                        targetWriteCapacity = TARGET_WCU_DURING_REPLICA;
                        resetTargetWCU = true;
                    }
                    else
                        targetWriteCapacity = writeCapacity;
                    request.setProvisionedThroughput(new ProvisionedThroughput(targetReadCapacity, targetWriteCapacity));

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
                    if(writeCapacity.compareTo(TARGET_WCU_DURING_REPLICA) < 0)
                        throw new Exception("Could not create table " + destinationTable + " with at least " + TARGET_WCU_DURING_REPLICA + " WCUs");
                }
                else
                    throw e;
            }

        }

        try {
            ExecutorService sourceExec = getSourceThreadPool(numSegments);
            ExecutorService destinationExec = getDestinationThreadPool(maxWriteThreads);

            AbstractLogConsumer consumer = null;
            AbstractLogProvider worker = null;
            if(targetIsHardDisk)
            {
                consumer = new DynamoDBConsumer2(destinationTable, destinationExec);
            }
            else{
                final double writeThroughput = calculateThroughput(
                        writeTableDescription, writeThroughputRatio, false);
                LOGGER.info("DynamoDBConsumer ratelimit = " + writeThroughput);
                consumer = new DynamoDBConsumer(destinationClient,
                        destinationTable, writeThroughput, destinationExec);
            }

            if(sourceIsHardDisk)
            {
                worker = new DynamoDBBootstrapWorker2(params.getMaxFileID(), numSegments, sourceTable, sourceExec);
            }
            else
            {
                final double readThroughput = calculateThroughput(readTableDescription,
                    readThroughputRatio, true);
                LOGGER.info("DynamoDBBootstrapWorker ratelimit = " + readThroughput);
                worker = new DynamoDBBootstrapWorker(
                        sourceClient, readThroughput, sourceTable, sourceExec,
                        params.getSection(), params.getTotalSections(), numSegments, consistentScan);
            }

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

            DescribeTableResult res = sourceClient.describeTable(sourceTable);
            readTableDescription = res.getTable();
            Long readCapacity = readTableDescription.getProvisionedThroughput().getReadCapacityUnits();
            if(!readCapacity.equals(sourceReadCapacity)){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                String msg="Could not reset to " + sourceReadCapacity + " RCUs for " + sourceTable;
                LOGGER.warn(msg);
                //throw new Exception(msg);
            }
        }

        if(resetTargetWCU == true)
        {
            UpdateTableRequest request = new UpdateTableRequest()
                .withTableName(destinationTable);
            request.setProvisionedThroughput(new ProvisionedThroughput(targetReadCapacity, targetWriteCapacity));

            try{
                UpdateTableResult response = destinationClient.updateTable(request);
                waitTillTableUpdated(destinationClient, destinationTable, response);
            }catch(Exception e){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                LOGGER.warn("Exception while resetting lower RCU, WCU for " + destinationTable + " " + e);
            }

            DescribeTableResult res = destinationClient.describeTable(destinationTable);
            writeTableDescription = res.getTable();
            Long writeCapacity = writeTableDescription.getProvisionedThroughput().getWriteCapacityUnits();
            if(!writeCapacity.equals(targetWriteCapacity)){
                //reducing provisioning has some limits (@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
                //since replication operation is successful, this failure can be ignored at risk of paying higher bill
                //till the higher provision is manually reduced at a later time!!!
                String msg="Could not reset to " + targetWriteCapacity + " WCUs for " + destinationTable;
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
