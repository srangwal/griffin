/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.control.GriffinControlManager;
import com.turn.griffin.data.GriffinDataManager;
import com.turn.griffin.utils.GriffinConfig;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import com.turn.griffin.utils.GriffinStringConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;


/*
    NOTE: The proper functioning of this module requires:
        - List of Zookeeper server
        - A list of a subset of Apache Kakfa brokers such that at least one of them is online at any given time
        - Auto topic creation enabled on Apache Kafka brokers
 */
public class GriffinModule implements Griffin {

    public static final Logger logger = LoggerFactory.getLogger(GriffinModule.class);
    public static final String PROPERTY_PREFIX = "Griffin.";


    /* This is an important parameter. All timing parameters are derived from it */
    public static final int RESOURCE_DISCOVERY_INTERVAL_MS =
            new GriffinRangedIntConfig(PROPERTY_PREFIX + "ResourceDiscoveryInterval",
                    "How often the global view is broadcast on the control channel (in ms)",
                    60000, 10000, 600000).getValue();

    /* Comma separated Zookeeper server list */
    public static final String ZOOKEEPER =
            GriffinConfig.getProperty(PROPERTY_PREFIX + "ZkServers",
                    System.getenv("ZK_PORT_2181_TCP_ADDR") + ":2181");

    /* Comma separated broker list */
    public static final String BROKERS =
            GriffinConfig.getProperty(PROPERTY_PREFIX + "KafkaBrokersList",
                    System.getenv("KAFKA_PORT_9092_TCP_ADDR") + ":9092");


    private static final String ERROR_EMAIL_RECIPIENTS = new GriffinStringConfig(
            PROPERTY_PREFIX + "Error.Email.Recipients",
            "Email addresses to report errors or warnings incurred in Griffin",
            "username@yourorg.com").getValue();

    private static final String ERROR_EMAIL_SENDER = new GriffinStringConfig(
            PROPERTY_PREFIX + "Error.Email.Sender",
            "Sender's email addresses to report errors or warnings incurred in Griffin",
            "username@yourorg.com").getValue();

    private String myServerId;

    private Optional<GriffinControlManager> controlManager = Optional.absent();
    private Optional<GriffinDataManager> dataManager = Optional.absent();
    private Optional<GriffinLibCacheUtil> libCacheManager = Optional.absent();


    public static Optional<GriffinModule> getInstance() {
        return Optional.fromNullable(GriffinInstance.instance);
    }

    public String getMyServerId() {
        return this.myServerId;
    }


    public Optional<GriffinLibCacheUtil> getLibCacheManager() {
        return this.libCacheManager;
    }

    public Optional<GriffinDataManager> getDataManager() {
        return this.dataManager;
    }


    private GriffinModule() {

        populateLocalVariables();
        Preconditions.checkState(!StringUtils.isBlank(this.myServerId), "Server id is not defined");
        Preconditions.checkState(!StringUtils.isBlank(ZOOKEEPER), "Zookeeper is not defined");
        Preconditions.checkState(!StringUtils.isBlank(BROKERS), "Brokers are not defined");

        this.libCacheManager = Optional.of(new GriffinLibCacheUtil(getMyServerId()));
        this.controlManager = Optional.of(new GriffinControlManager(this));
        this.dataManager = Optional.of(new GriffinDataManager(this));

    }

    private void populateLocalVariables() {
        try {
            this.myServerId = GriffinConfig.getProperty("serverid", null);
        } catch (Exception ex) {
            Preconditions.checkState(false, String.format("Failed to initialize GriffinModule, %s", ex));
        }
    }

    public synchronized void shutdown() {

        if (this.controlManager.isPresent()) {
            this.controlManager.get().shutdown();
        }

        if (this.dataManager.isPresent()) {
            this.dataManager.get().shutdown();
        }
    }

    /* Send a request on the control channel about electing an uploader for the blob */
    public void sendFileUploadRequest(FileInfo fileInfo) {
        controlManager.get().sendFileUploaderRequest(fileInfo);
    }

    /* TODO: add a function that takes a blob of bytes rather than a filename
       public boolean syncBlob(String blobName, String dest, byte[] bytes) {
     */

    @Override
    public boolean syncBlob(String blobName, String dest, String filepath) {

        Preconditions.checkState(!StringUtils.isBlank(blobName));
        Preconditions.checkState(!StringUtils.isBlank(filepath));
        Preconditions.checkState(!StringUtils.isBlank(dest));

        File file = new File(filepath);
        if ( !file.exists()) {
            logger.warn(String.format("File %s does not exists", filepath));
            return false;
        }

        if (file.length() == 0 || file.length() > getMaxBlobSizePermitted()) {
            logger.warn(String.format("File %s is greater the maximum allowed size of %s",
                    filepath, getMaxBlobSizePermitted()));
            String subject = String.format("ALERT: Griffin syncBlob rejected blob:%s", blobName);
            String body = String.format("Action: syncBlob rejected blob:%s filepath:%s\n" +
                    "Reason: Size of %s bytes does not lie in (0,%s] \n", blobName, filepath,
                    file.length(), getMaxBlobSizePermitted());
            emailAlert(subject, body);
            return false;
        }

        Optional<FileInfo> fileInfo = libCacheManager.get().addFileToLocalLibCache(blobName, dest, filepath);
        if (fileInfo.isPresent()) {
            logger.info(String.format("Added %s",
                    fileInfo.get().toString().replaceAll(System.getProperty("line.separator"), " ")));
            dataManager.get().uploadFile(fileInfo.get());
            return true;
        } else {
            logger.error(String.format("Unable to add %s with destination %s to local repository ", blobName, dest));
            String subject = String.format("ALERT: Griffin syncBlob rejected blob:%s", blobName);
            String body = String.format("Action: syncBlob rejected blob:%s filepath:%s\n" +
                    "Reason: unable to add file to local repository\n", blobName, filepath);
            emailAlert(subject, body);
            return false;
        }
    }

    private long getMaxBlobSizePermitted() {
        return new GriffinRangedIntConfig(PROPERTY_PREFIX + "MaxBlobSizeInGB",
                "Maximum size of the blob (in GB) allowed in Griffin",
                2, 1, 10).getValue() * FileUtils.ONE_GB;
    }

    public String multiConcat(List<String> pathElements) {
        String fullConcatName = pathElements.get(0);
        for (String name: pathElements.subList(1, pathElements.size())) {
            fullConcatName = FilenameUtils.concat(fullConcatName, name);
        }
        return fullConcatName;
    }

    public static void emailAlert(String subject, String body) {
        String serverId = GriffinConfig.getProperty("serverid", null);
        try {
            SimpleEmail email = new SimpleEmail();
            email.setCharset("utf-8");
            email.setFrom(ERROR_EMAIL_SENDER);
            email.addTo(ERROR_EMAIL_RECIPIENTS);
            email.setSubject(subject + "(" + serverId + ")");
            email.setMsg(body);
            email.send();
        } catch (EmailException e) {
            logger.error(String.format("Failed to send alert email To:%s Subject:%s Body:%s", ERROR_EMAIL_RECIPIENTS,
                    subject, body), e);
        }
    }

    private static class GriffinInstance {
        public static GriffinModule instance = new GriffinModule();
    }
}
