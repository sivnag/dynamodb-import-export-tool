/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.beust.jcommander.Parameter;

/**
 * This class contains the parameters to input when executing the program from
 * command line.
 */
public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "Endpoint of the source table", required = true)
    private String sourceEndpoint;

    public String getSourceEndpoint() {
        return sourceEndpoint;
    }

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "Endpoint of the destination table", required = true)
    private String destinationEndpoint;

    public String getDestinationEndpoint() {
        return destinationEndpoint;
    }

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public String getDestinationTable() {
        return destinationTable;
    }

    public static final String CREATE_DESTINATION_TABLE_IF_NOT_FOUND = "--createDestinationTableIfNotFound";
    @Parameter(names = CREATE_DESTINATION_TABLE_IF_NOT_FOUND, description = "if destinationTable is not found in destinationEndpoint, then, create destinationTable based on sourceTable")
    private boolean createDestinationTableIfNotFound = false;

    public boolean shouldCreateDestinationTableIfNotFound() {
        return createDestinationTableIfNotFound;
    }

    public static final String READ_THROUGHPUT_RATIO = "--readThroughputRatio";
    @Parameter(names = READ_THROUGHPUT_RATIO, description = "Percentage of total read throughput to scan the source table", required = true)
    private double readThroughputRatio;

    public double getReadThroughputRatio() {
        return readThroughputRatio;
    }

    public static final String WRITE_THROUGHPUT_RATIO = "--writeThroughputRatio";
    @Parameter(names = WRITE_THROUGHPUT_RATIO, description = "Percentage of total write throughput to write the destination table", required = true)
    private double writeThroughputRatio;

    public double getWriteThroughputRatio() {
        return writeThroughputRatio;
    }

    public static final String MAX_WRITE_THREADS = "--maxWriteThreads";
    @Parameter(names = MAX_WRITE_THREADS, description = "Number of max threads to write to destination table", required = false)
    private int maxWriteThreads = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE;

    public int getMaxWriteThreads() {
        return maxWriteThreads;
    }

    public static final String TOTAL_SECTIONS = "--totalSections";
    @Parameter(names = TOTAL_SECTIONS, description = "Total number of sections to divide the scan into", required = false)
    private int totalSections = 1;

    public int getTotalSections() {
        return totalSections;
    }

    public static final String SECTION = "--section";
    @Parameter(names = SECTION, description = "Section number to scan when running multiple programs concurrently [0, 1... totalSections-1]", required = false)
    private int section = 0;

    public int getSection() {
        return section;
    }
    
    public static final String CONSISTENT_SCAN = "--consistentScan";
    @Parameter(names = CONSISTENT_SCAN, description = "Use this flag to use strongly consistent scan. If the flag is not used it will default to eventually consistent scan")
    private boolean consistentScan = false;

    public boolean getConsistentScan() {
        return consistentScan;
    }

    public static final String NUM_SEGMENTS = "--numSegments";
    @Parameter(names = NUM_SEGMENTS, description = "total number of segments to be used in parallel scan of table", required = false)
    private int numSegments = 0;

    public int getNumSegments() {
        return numSegments;
    }

    public static final String MAX_FILE_ID = "--max-file-id";
    @Parameter(names = MAX_FILE_ID, description = "when sourceEndpoint is 'HardDisk' this option specifies the maximum file ID such that <sourceTable>_<max-file-id>.json exists in current working directory", required = false)
    private int maxFileID = 0;

    public int getMaxFileID() {
        return maxFileID;
    }

    public static final String MIN_SOURCE_RCU_DURING_REPLICA = "--min-source-rcu-during-replica";
    @Parameter(names = MIN_SOURCE_RCU_DURING_REPLICA, description = "minimum RCU provisioning for source dynamodb table", required = false)
    private Long minSourceRCU = 500;

    public Long getMinSourceRCU() {
        return minSourceRCU;
    }

    public static final String MIN_TARGET_WCU_DURING_REPLICA = "--min-target-wcu-during-replica";
    @Parameter(names = MIN_TARGET_WCU_DURING_REPLICA, description = "minimum WCU provisioning for target dynamodb table", required = false)
    private Long minTargetWCU = 500;

    public Long getMinTargetWCU() {
        return minTargetWCU;
    }
}
