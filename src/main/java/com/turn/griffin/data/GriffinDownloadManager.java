/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.data;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Identify files to download and schedule GriffinDownloadTask to download it.
 *
 * @author srangwala
 */
public final class GriffinDownloadManager implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(GriffinDownloadManager.class);

    private static final String SEPARATOR = "-";
    private static final Random RANDOM = new Random();
    private static final String MODULENAME = "turn.griffin";
    private static final String METRICNAME = "cache";
    private final int deleteTopicsEveryNRuns;


    protected static final Map<String, ScheduledFuture<?>> ONGOING_DOWNLOADS = new ConcurrentHashMap<>();
    private GriffinDataManager dataManager;

    public GriffinDownloadManager(GriffinDataManager dataManager) {
        Preconditions.checkNotNull(dataManager);
        this.dataManager = dataManager;
        /* Once per day */
        this.deleteTopicsEveryNRuns = (int) (TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)
                / dataManager.NEWFILE_CHECK_INTERVAL_MS);
    }

    @Override
    public void run() {

        try {
            /* Clear up finished or cancelled downloads */
            for (Map.Entry<String, ScheduledFuture<?>> entry : ONGOING_DOWNLOADS.entrySet()) {
                String key = entry.getKey();
                ScheduledFuture<?> future = entry.getValue();
                if (future.isCancelled() || future.isDone()) {
                    logger.info(String.format("Finished download: %s", key));
                    ONGOING_DOWNLOADS.remove(key);
                } else {
                /* For Debugging */
                    logger.info(String.format("Ongoing download: %s", key));
                }
            }

            /* Delete kafka topics of expired versions. Do it very infrequently (approx once every day )
            since it needs to connect to Kafka and a delay in removing the topics is OK */
            boolean deleteExpiredKafkaTopics = RANDOM.nextInt(deleteTopicsEveryNRuns) == 0;
            /* Clear older versions*/
            deleteExpiredVersions(deleteExpiredKafkaTopics);

            Map<String, FileInfo> filesToDownload = dataManager.getLibCacheManager().get().findFilesToDownload();
            logger.debug("Files to download " + filesToDownload.keySet());
            for (Map.Entry<String, FileInfo> entry : filesToDownload.entrySet()) {
                String filename = entry.getKey();
                FileInfo fileInfo = entry.getValue();

                long version = fileInfo.getVersion();
                String fileKey = getFileKey(filename, version);

                if (ONGOING_DOWNLOADS.get(fileKey) == null) {
                    logger.info(String.format("Scheduling download for %s",
                            fileInfo.toString().replaceAll(System.getProperty("line.separator"), " ")));

                    ONGOING_DOWNLOADS.put(fileKey, dataManager.scheduleDataDownloadJob(
                            new GriffinDownloadTask(dataManager, fileInfo),
                            RANDOM.nextInt(dataManager.getDownloadDelay()), TimeUnit.MILLISECONDS));
                }
            }
        } catch (RuntimeException e) {
            /* It is important to catch any exception in run() because this class is scheduled to
               run with fixed delay and a uncaught exception will prevent run() to run repeatedly
             */
            logger.info("Runtime exception", e);
        }
    }

    public String getMyServerId() {
        return this.dataManager.getMyServerId();
    }

    public void deleteExpiredVersions(boolean deleteExpiredKafkaTopics) {

        GriffinLibCacheUtil libCacheManager = dataManager.getLibCacheManager().get();

        /* Delete expired version in local repository */
        libCacheManager.deleteExpiredLibCacheVersions();

        if (deleteExpiredKafkaTopics) {
            try {
                deleteExpiredKafkaTopics(libCacheManager);
            } catch (Exception e) {
                logger.error("Unable to delete expired kafka topics", e);
            }
        }
    }

    /* Delete Kafka topics for expired file version */
    private void deleteExpiredKafkaTopics(GriffinLibCacheUtil libCacheManager) {

        /* Delete Kafka topics of all expired versions */
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, GriffinModule.BROKERS))) {
            List<String> topicList = new ArrayList<>(adminClient.listTopics().names().get());
            for(Map.Entry<String, String> entry : libCacheManager.getLocalFileLatestVersion().entrySet()) {
                /* Delete all versions in kafka except the latest version */
                deleteKafkaTopics(adminClient, topicList, entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("Unable to list kafka topics", e);
        }
    }

    /* Delete all topics in Kafka for versions older than the specified version */
    private void deleteKafkaTopics(final AdminClient adminClient, final List<String> topicList,
                                   final String filename, final String version) {

        final String topicPattern = GriffinKafkaTopicNameUtil.getDataTopicNamePattern(filename);

        Collection<String> topicsToDelete = Collections2.filter(topicList,
                new Predicate<String>() {
                    @Override
                    public boolean apply(String topic) {
                        return Pattern.matches(topicPattern, topic) &&
                                (GriffinKafkaTopicNameUtil.getVersionFromDataTopicName(topic) <
                                        Long.parseLong(version));
                    }
                }
        );

        for (String topic: topicsToDelete) {
            logger.info("Deleting kafka topic:" + topic);
            try {
                adminClient.deleteTopics(Collections.singleton(topic)).all().get();
            } catch (Exception e) {
                /* Deleted topics can reappear because we operate with auto-topic creation enabled in kafka.
                   Furthermore, multiple nodes might try to delete the topic at the same time. Since there is
                   no harm in multiple deletion ignore topic-not-found errors.
                 */
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    logger.warn(String.format("Topic deletion of topic %s after it has been deleted", topic));
                } else {
                    logger.warn(String.format("Unable to delete kafka topic %s", topic), e);
                }
            }
        }
    }


    private String getFileKey(String filename, long version) {
        return filename + SEPARATOR + version;
    }



}
