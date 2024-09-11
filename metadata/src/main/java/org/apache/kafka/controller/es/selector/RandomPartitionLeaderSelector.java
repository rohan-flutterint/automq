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

package org.apache.kafka.controller.es.selector;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RandomPartitionLeaderSelector implements PartitionLeaderSelector {
    private final List<Integer> aliveBrokers;
    private int selectNextIndex = 0;

    public RandomPartitionLeaderSelector(List<Integer> aliveBrokers, Predicate<Integer> predicate) {
        this.aliveBrokers = aliveBrokers.stream().filter(predicate).collect(Collectors.toList());
        Collections.shuffle(this.aliveBrokers);
    }

    @Override
    public Optional<Integer> select(TopicPartition tp) {
        if (aliveBrokers.isEmpty()) {
            return Optional.empty();
        }
        int broker = aliveBrokers.get(selectNextIndex);
        selectNextIndex = (selectNextIndex + 1) % aliveBrokers.size();
        return Optional.of(broker);
    }
}
