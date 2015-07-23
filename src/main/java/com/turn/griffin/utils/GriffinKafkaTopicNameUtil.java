/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.base.Optional;

/** A utility class to manage kafka topic names for Griffin
* @author srangwala
*/
public class GriffinKafkaTopicNameUtil {

    public static final String KAFKA_TOPIC_PREFIX = "GRIFFIN";
    public static final String KAFKA_TOPIC_SEPARATOR = "-";
    public static final String CONTROL_TOPIC_NAME = "CONTROL";
    public static final Optional<String> localDataCenter =
            Optional.fromNullable(GriffinConfig.getProperty("dcname", null));


    private GriffinKafkaTopicNameUtil() {
        /* Util class, prevent initialization */
    }

    /* Data Topics */
    public static String getDataTopicConsumerGroupId(String filename, long version, String[] suffixes) {
        String groupId = getDataTopicNameForProducer(filename, version);
        for (String suffix: suffixes) {
            groupId += KAFKA_TOPIC_SEPARATOR + suffix;
        }
        return groupId;
    }

    public static String getDataTopicNamePattern(String filename) {
        return ".*" + KAFKA_TOPIC_SEPARATOR + KAFKA_TOPIC_PREFIX + KAFKA_TOPIC_SEPARATOR
                + filename + KAFKA_TOPIC_SEPARATOR + ".*$";

    }

    public static String getDataTopicNameForConsumer(String filename, long version) {
        return getDataTopicNameForConsumer(filename, Long.toString(version));
    }

    public static String getDataTopicNameForConsumer(String filename, String version) {
        return ".*" + KAFKA_TOPIC_SEPARATOR + KAFKA_TOPIC_PREFIX + KAFKA_TOPIC_SEPARATOR
                + filename + KAFKA_TOPIC_SEPARATOR + version;
    }

    public static String getDataTopicNameForProducer(String filename, long version) {
        return localDataCenter.get().toUpperCase() + KAFKA_TOPIC_SEPARATOR + KAFKA_TOPIC_PREFIX
                + KAFKA_TOPIC_SEPARATOR + filename + KAFKA_TOPIC_SEPARATOR + version;
    }

    public static long getVersionFromDataTopicName(String topic) {
        String[] topicSplit = topic.split(KAFKA_TOPIC_SEPARATOR);
        return Long.parseLong(topicSplit[topicSplit.length - 1]);
    }


    /* Control Topic */
    public static String getControlTopicConsumerGroupId(String[] suffixes) {
        String groupId = getControlTopicNameForProducer();
        for (String suffix: suffixes) {
            groupId += KAFKA_TOPIC_SEPARATOR + suffix;
        }
        return groupId;
    }

    public static String getControlTopicNameForConsumer() {
        return ".*" + KAFKA_TOPIC_SEPARATOR + KAFKA_TOPIC_PREFIX + KAFKA_TOPIC_SEPARATOR + CONTROL_TOPIC_NAME;
    }

    public static String getControlTopicNameForProducer() {
        return localDataCenter.get().toUpperCase() + KAFKA_TOPIC_SEPARATOR + KAFKA_TOPIC_PREFIX
                + KAFKA_TOPIC_SEPARATOR + CONTROL_TOPIC_NAME;
    }


}
