/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.autobalancer.metricsreporter.metric;

import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.metricsreporter.exception.UnknownVersionException;

import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric.
 */
public class TopicPartitionMetrics extends AutoBalancerMetrics {
    private final String topic;
    private final int partition;

    public TopicPartitionMetrics(long time, int brokerId, String brokerRack, String topic, int partition) {
        this(time, brokerId, brokerRack, topic, partition, new HashMap<>());
    }

    public TopicPartitionMetrics(long time, int brokerId, String brokerRack, String topic, int partition, Map<Byte, Double> metricsMap) {
        super(time, brokerId, brokerRack, metricsMap);
        this.topic = topic;
        this.partition = partition;
    }

    public static TopicPartitionMetrics fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
        byte version = buffer.get();
        if (version > METRIC_VERSION) {
            throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                    + "Current version is " + METRIC_VERSION);
        }
        long time = buffer.getLong();
        int brokerId = buffer.getInt();

        int brokerRackLength = buffer.getInt();
        String brokerRack = "";
        if (brokerRackLength > 0) {
            brokerRack = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), brokerRackLength, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + brokerRackLength);
        }
        int topicLength = buffer.getInt();
        String topic = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), topicLength, StandardCharsets.UTF_8);
        buffer.position(buffer.position() + topicLength);
        int partition = buffer.getInt();
        Map<Byte, Double> metricsMap = parseMetricsMap(buffer);
        return new TopicPartitionMetrics(time, brokerId, brokerRack, topic, partition, metricsMap);
    }

    @Override
    public String key() {
        return MetricsUtils.topicPartitionKey(topic, partition);
    }

    @Override
    public byte metricType() {
        return MetricTypes.TOPIC_PARTITION_METRIC;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    /**
     * The buffer capacity is calculated as follows:
     * <ul>
     *   <li>(headerPos + {@link Byte#BYTES}) - version</li>
     *   <li>{@link Long#BYTES} - time</li>
     *   <li>{@link Integer#BYTES} - broker id</li>
     *   <li>{@link Integer#BYTES} - broker rack length</li>
     *   <li>brokerRack.length - broker rack</li>
     *   <li>{@link Integer#BYTES} - topic length</li>
     *   <li>topic.length - topic</li>
     *   <li>{@link Integer#BYTES} - partition</li>
     *   <li>body length - metric-value body</li>
     * </ul>
     *
     * @param headerPos Header position
     * @return Byte buffer of the partition metric.
     */
    @Override
    public ByteBuffer toBuffer(int headerPos) {
        byte[] brokerRackBytes = brokerRack().getBytes(StandardCharsets.UTF_8);
        byte[] topic = topic().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(headerPos + Byte.BYTES
                + Long.BYTES
                + Integer.BYTES
                + Integer.BYTES
                + brokerRackBytes.length
                + Integer.BYTES
                + topic.length
                + Integer.BYTES
                + bodySize());
        buffer.position(headerPos);
        buffer.put(METRIC_VERSION);
        buffer.putLong(time());
        buffer.putInt(brokerId());
        buffer.putInt(brokerRackBytes.length);
        if (brokerRackBytes.length > 0) {
            buffer.put(brokerRackBytes);
        }
        buffer.putInt(topic.length);
        buffer.put(topic);
        buffer.putInt(partition);

        buffer = writeBody(buffer);
        return buffer;
    }

    @Override
    public String toString() {
        return String.format("[TopicPartitionMetrics,Time=%d,BrokerId=%d,Partition=%s,Key:Value=%s]",
                time(), brokerId(), new TopicPartition(topic(), partition()), buildKVString());
    }
}
