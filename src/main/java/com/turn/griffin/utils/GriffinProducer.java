/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * A class to create a producer to produce protobuf messages
 *
 * @author srangwala
 */
public class GriffinProducer {

    private static final Logger logger = LoggerFactory.getLogger(GriffinProducer.class);
    private static final Random RANDOM_KEY = new Random();
    private byte[] key = new byte[2];
    private KafkaProducer<byte[], byte[]> producer;


    public GriffinProducer(String brokers) {
        this(brokers, "kafka.producer.DefaultPartitioner");
    }

    public GriffinProducer(String brokers, String partitioner) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");

        if (!"kafka.producer.DefaultPartitioner".equals(partitioner)) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        }

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String topic, Message message) {
        /* Give a random key to spread the messages across all partitions */
        /* This makes it thread-unsafe but that's OK since we just need some random value in this.key */
        RANDOM_KEY.nextBytes(this.key);
        sendRecord(new ProducerRecord<>(topic, this.key.clone(), message.toByteArray()));
    }

    public void send(String topic, String key, Message message) {
        sendRecord(new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), message.toByteArray()));
    }

    public void send(String topic, List<Message> messages) {
        List<ProducerRecord<byte[], byte[]>> kMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            /* Give a random key to spread the messages across all partitions */
            /* This makes it thread-unsafe but that's OK since we just need some random value in this.key */
            RANDOM_KEY.nextBytes(this.key);
            kMessages.add(new ProducerRecord<>(topic, this.key.clone(), message.toByteArray()));
        }
        for (ProducerRecord<byte[], byte[]> kMessage: kMessages) {
            sendRecord(kMessage);
        }
    }

    public void shutdown() {
        this.producer.close(Duration.ofMillis(Long.MAX_VALUE));
    }

    private void sendRecord(ProducerRecord<byte[], byte[]> record) {
        try {
            this.producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrupted while sending Kafka message", e);
        } catch (ExecutionException e) {
            throw new KafkaException("Unable to send Kafka message", e);
        }
    }

}
