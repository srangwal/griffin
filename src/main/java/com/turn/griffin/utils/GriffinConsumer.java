/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 *
 * <p>
 * A class that collects all messages for a given topic list (given as a regex) into a specified queue
 * using a local thread pool.
 *
 * @author srangwala
 */

public class GriffinConsumer {

    private static final Logger logger = LoggerFactory.getLogger(GriffinConsumer.class);

    private String brokers;
    private String groupId;
    private String topicRegEx;
    private BlockingQueue<byte[]> msgQueue;
    private ExecutorService kafkaStreamsExecutor;


    public GriffinConsumer(String brokers, String groupId, String topicRegEx,
                           int consolidationThreadCount, Properties props,
                           BlockingQueue<byte[]> msgQueue) {

        Preconditions.checkState(!StringUtils.isBlank(brokers), "Kafka brokers are not defined");
        Preconditions.checkState(!StringUtils.isBlank(groupId), "Group id is not defined");
        Preconditions.checkState(!StringUtils.isBlank(topicRegEx), "Topic is not defined");
        Preconditions.checkNotNull(msgQueue);

        this.brokers = brokers;
        this.groupId = groupId;
        this.topicRegEx = topicRegEx;
        this.msgQueue = msgQueue;

        this.run(consolidationThreadCount, props);
    }

    private static Properties createConsumerConfig(String brokers, String groupId, Properties userProps) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "300000");  // 5 min
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "20000");

        /* Add user specified properties */
        /* Should be the last line to allow overriding any of the properties above */
        props.putAll(userProps);

        if ("smallest".equals(props.get("auto.offset.reset"))) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        return props;
    }

    private void run(int threadCount, Properties props) {

        /* Create a thread pool */
        this.kafkaStreamsExecutor = Executors.newFixedThreadPool(threadCount);

        logger.debug(String.format("Consuming topic:%s with %s streams", this.topicRegEx, threadCount));
        for (int i = 0; i < threadCount; i++) {
            kafkaStreamsExecutor.submit(new KafkaConsumer(createConsumerConfig(
                    this.brokers, this.groupId, props), this.topicRegEx, this.msgQueue));
        }
    }

    public void shutdown(boolean clearGroup) {
        /* It is important to call shutdownNow so that blocking threads get an interrupt signal */
        this.kafkaStreamsExecutor.shutdownNow();
        try {
            this.kafkaStreamsExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (clearGroup) {
            try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers))) {
                adminClient.deleteConsumerGroups(Collections.singleton(this.groupId)).all().get();
            } catch (Exception e) {
                if (!(e.getCause() instanceof GroupIdNotFoundException)) {
                    logger.debug("Unable to delete Kafka consumer group " + this.groupId, e);
                }
            }
        }
    }

    public class KafkaConsumer implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

        private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer;
        private String topicRegEx;
        private BlockingQueue<byte[]> msgQueue;

        public KafkaConsumer(Properties props, String topicRegEx, BlockingQueue<byte[]> msgQueue) {
            this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
            this.topicRegEx = topicRegEx;
            this.msgQueue = msgQueue;
        }

        public void run() {
            this.consumer.subscribe(Pattern.compile(this.topicRegEx));

            try {
                while (true) {
                    if (isInterrupted())
                        break;
                    ConsumerRecords<byte[], byte[]> records = this.consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<byte[], byte[]> record: records) {
                        this.msgQueue.put(record.value());
                    }
                }
            } catch (InterruptedException ie) {
                logger.warn("GriffinConsumer interrupted. Stopping KafkaConsumer.");
            } catch (InterruptException ie) {
                logger.warn("GriffinConsumer interrupted. Stopping KafkaConsumer.");
            } catch (Exception e) {
                logger.info("Exception in KafkaConsumer", e);
            } finally {
                this.consumer.close();
            }

            logger.debug(String.format("Shutting KafkaConsumer for %s", this.topicRegEx));
        }

        private boolean isInterrupted() {
            boolean isInterrupted = Thread.currentThread().isInterrupted();
            if (isInterrupted) {
                logger.debug(String.format("Kafka consumer thread interrupted %s", this.topicRegEx));
            }
            return isInterrupted;

        }
    }
}
