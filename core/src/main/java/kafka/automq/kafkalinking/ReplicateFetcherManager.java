/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.kafkalinking;

import kafka.server.AbstractFetcherManager;
import kafka.server.AbstractFetcherThread;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public abstract class ReplicateFetcherManager<T extends AbstractFetcherThread> extends AbstractFetcherManager<T> {
    public ReplicateFetcherManager(String name, String clientId, int numFetchers) {
        super(name, clientId, numFetchers);
    }

    public abstract void addPartitions(Set<TopicPartition> topicPartitions);
}
