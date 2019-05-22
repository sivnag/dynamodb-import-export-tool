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

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(CommandLineInterface.class);

    private static void waitTillTableCreated(AmazonDynamoDBClient client, String tableName, CreateTableResult response) throws java.lang.InterruptedException
    {
        TableDescription tableDescription = response.getTableDescription();

        String status = tableDescription.getTableStatus();

        System.out.println(tableName + " - " + status);

        while (!status.equals("ACTIVE")){
            Thread.sleep(5000);
            try{
                DescribeTableResult res = client.describeTable(tableName);
                status = res.getTable().getTableStatus();
                System.out.println(tableName + " - " + status);
            }
            catch (ResourceNotFoundException e){}
        }
    }

    private static GlobalSecondaryIndex GlobalSecondaryIndexDescriptionToGlobalSecondaryIndex(GlobalSecondaryIndexDescription indexDescription){
        return new GlobalSecondaryIndex()
            .withIndexName(indexDescription.getIndexName())
            .withKeySchema(indexDescription.getKeySchema())
            .withProjection(indexDescription.getProjection())
            .withProvisionedThroughput(new ProvisionedThroughput(indexDescription.getProvisionedThroughput().getReadCapacityUnits(),
                indexDescription.getProvisionedThroughput().getWriteCapacityUnits()));
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
    public static void main(String[] args) throws java.lang.InterruptedException{
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
        final String sourceEndpoint = params.getSourceEndpoint();
        final String destinationEndpoint = params.getDestinationEndpoint();
        final String destinationTable = params.getDestinationTable();
        final String sourceTable = params.getSourceTable();
        final double readThroughputRatio = params.getReadThroughputRatio();
        final double writeThroughputRatio = params.getWriteThroughputRatio();
        final int maxWriteThreads = params.getMaxWriteThreads();
        final boolean consistentScan = params.getConsistentScan();

        final ClientConfiguration sourceConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);
        final ClientConfiguration destinationConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);

        final AmazonDynamoDBClient sourceClient = new AmazonDynamoDBClient(
                new DefaultAWSCredentialsProviderChain(), sourceConfig);
        final AmazonDynamoDBClient destinationClient = new AmazonDynamoDBClient(
                new DefaultAWSCredentialsProviderChain(), destinationConfig);
        sourceClient.setEndpoint(sourceEndpoint);
        destinationClient.setEndpoint(destinationEndpoint);

        TableDescription readTableDescription = sourceClient.describeTable(
                sourceTable).getTable();
        TableDescription writeTableDescription;
        try{
            writeTableDescription = destinationClient
                    .describeTable(destinationTable).getTable();
        }catch(ResourceNotFoundException e){
            if(params.shouldCreateDestinationTableIfNotFound()){
                LOGGER.warn("destination table " + destinationTable + " not found. Creating using source table description...");
                CreateTableRequest request = new CreateTableRequest()
                    .withAttributeDefinitions(readTableDescription.getAttributeDefinitions())
                    .withTableName(destinationTable)
                    .withKeySchema(readTableDescription.getKeySchema())
                    .withProvisionedThroughput(new ProvisionedThroughput(
                        readTableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                        readTableDescription.getProvisionedThroughput().getWriteCapacityUnits()));

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
            }
            else
                throw e;
        }
        int numSegments = 10;
        try {
            numSegments = DynamoDBBootstrapWorker
                    .getNumberOfSegments(readTableDescription);
        } catch (NullReadCapacityException e) {
            LOGGER.warn("Number of segments not specified - defaulting to "
                    + numSegments, e);
        }

        final double readThroughput = calculateThroughput(readTableDescription,
                readThroughputRatio, true);
        final double writeThroughput = calculateThroughput(
                writeTableDescription, writeThroughputRatio, false);

        try {
            ExecutorService sourceExec = getSourceThreadPool(numSegments);
            ExecutorService destinationExec = getDestinationThreadPool(maxWriteThreads);
            DynamoDBConsumer consumer = new DynamoDBConsumer(destinationClient,
                    destinationTable, writeThroughput, destinationExec);

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
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     */
    private static double calculateThroughput(
            TableDescription tableDescription, double throughputRatio,
            boolean read) {
        if (read) {
            return tableDescription.getProvisionedThroughput()
                    .getReadCapacityUnits() * throughputRatio;
        }
        return tableDescription.getProvisionedThroughput()
                .getWriteCapacityUnits() * throughputRatio;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    private static ExecutorService getDestinationThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }
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

        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                numSegments, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(numSegments),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

}
