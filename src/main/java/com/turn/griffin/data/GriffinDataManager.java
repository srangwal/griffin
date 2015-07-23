/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.data;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *  Manages all data channel related actions.
 *
 * @author  srangwala
 */
public class GriffinDataManager {

    public static final Logger logger = LoggerFactory.getLogger(GriffinDataManager.class);

    /* This parameter is critical for the proper functioning of Griffin. This
     * value should be equal to the max number of simultaneous files (collectively)
     * pushed from all the nodes using Griffin.
     * */
    private final int DATA_THREAD_POOL_COUNT =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "DataThreadPoolCount",
                    "Number of threads for griffin's data tasks (upload and downloads)",
                    10, 5, 100).getValue();


    /* How often we would check if the local libcache has all the files as dictated by the global view */
    public final int NEWFILE_CHECK_INTERVAL_MS = 2 * GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS; // in ms

    private GriffinModule griffinModule;
    private ScheduledExecutorService dataUploadThreadPool;
    private ScheduledExecutorService dataDownloadThreadPool;

    public GriffinDataManager(GriffinModule griffinModule) {

        Preconditions.checkNotNull(griffinModule);
        this.griffinModule = griffinModule;

        /* Thread pool for checking new files to download and downloading them */
        this.dataUploadThreadPool = Executors.newScheduledThreadPool(2 * DATA_THREAD_POOL_COUNT);
        this.dataDownloadThreadPool = Executors.newScheduledThreadPool(DATA_THREAD_POOL_COUNT);

        logger.info(String.format("Starting Download manager every %s ms", NEWFILE_CHECK_INTERVAL_MS));
        this.dataDownloadThreadPool.scheduleWithFixedDelay(
                new GriffinDownloadManager(this), new Random().nextInt(NEWFILE_CHECK_INTERVAL_MS),
                NEWFILE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }


    public Optional<GriffinLibCacheUtil> getLibCacheManager() {
        return this.griffinModule.getLibCacheManager();
    }

    public String getMyServerId() {
        return this.griffinModule.getMyServerId();
    }

    public int getDownloadDelay() {
        /* Jitter before starting a download in milliseconds */
        return GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS /4;
    }

    public void sendFileUploadRequest(FileInfo fileInfo) {
        griffinModule.sendFileUploadRequest(fileInfo);
    }


    public ScheduledFuture<?> uploadFile(FileInfo fileInfo) {
        return scheduleDataUploadJob(new GriffinUploadTask(this, fileInfo), 0, TimeUnit.SECONDS);
    }

    private ScheduledFuture<?> scheduleDataUploadJob(Runnable runnable, long delay, TimeUnit unit) {
        return this.dataUploadThreadPool.schedule(runnable, delay, unit);
    }

    public ScheduledFuture<?> scheduleDataDownloadJob(Runnable runnable, long delay, TimeUnit unit) {
        return this.dataDownloadThreadPool.schedule(runnable, delay, unit);
    }

    public void shutdown() {
        this.dataUploadThreadPool.shutdown();
        this.dataDownloadThreadPool.shutdown();
    }

}
