/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.Executor;

public class DynamoDBFileScan {

    public DynamoDBFileScan() {
    }

    public ParallelScanExecutor2 getParallelScanCompletionService(String fileName, int maxFileID, int numSegments, Executor executor){
        final int segments = Math.max(1, numSegments);
        final ParallelScanExecutor2 completion = new ParallelScanExecutor2(
                executor, segments);

        //we will always scan all the files in local harddisk current working directory
        //if someone wants to keep the files distributed acorss multiple machines
        //then they have to carefully keep sections of files across machines (not entire set of files in each machine)

        int fileIDsPerWorker = Math.max(1,maxFileID/segments);
        for (int segment = 0; segment < segments; segment++) {
            int startFileID = 1 + segment*fileIDsPerWorker;
            int endFileID = startFileID + fileIDsPerWorker - 1;
            if(startFileID > maxFileID){
                startFileID = 0;
                endFileID = 0;
            }
            else if(endFileID > maxFileID || segment == segments-1)
                endFileID = maxFileID;

            completion.addWorker(new ScanSegmentWorker2(fileName, startFileID, endFileID, segment), segment);
        }

        return completion;
    }
}
