/*
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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.Semaphore;
import java.time.Duration;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.REPLICATION_DIRECTION_SWITCH_POLICY_CLASS;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private static final int MAX_OUTSTANDING_OFFSET_SYNCS = 10;

    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaProducer<byte[], byte[]> offsetProducer;
    private String sourceClusterAlias;
    private String offsetSyncsTopic;
    private Duration pollTimeout;
    private long maxOffsetLag;
    private Map<TopicPartition, PartitionState> partitionStates;
    private ReplicationPolicy replicationPolicy;
    private MirrorMetrics metrics;
    private boolean stopping = false;
    private Semaphore outstandingOffsetSyncs;
    private Semaphore consumerAccess;

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(String sourceClusterAlias, ReplicationPolicy replicationPolicy, long maxOffsetLag) {
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.maxOffsetLag = maxOffsetLag;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorTaskConfig config = new MirrorTaskConfig(props);
        outstandingOffsetSyncs = new Semaphore(MAX_OUTSTANDING_OFFSET_SYNCS);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        maxOffsetLag = config.maxOffsetLag();
        replicationPolicy = config.replicationPolicy();
        partitionStates = new HashMap<>();
        offsetSyncsTopic = config.offsetSyncsTopic();
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig());
        offsetProducer = MirrorUtils.newProducer(config.sourceProducerConfig());
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        Map<TopicPartition, Long> topicPartitionOffsets;

        boolean replicationDirectionSwitchEnable = config.replicationDirectionSwitchEnable();
        log.info("==replication.direction.switch.enable: {}", replicationDirectionSwitchEnable);
        if (replicationDirectionSwitchEnable) {
            log.info("=={}", config.getReplicationPolicy());
            if (!config.getReplicationPolicy().trim().equals(REPLICATION_DIRECTION_SWITCH_POLICY_CLASS)) {
                log.error("When replication.direction.switch.enable=true, must use IdentityReplicationPolicy");
                System.exit(0);
            }
            topicPartitionOffsets = loadSyncOffsets(config, taskTopicPartitions);
        } else {
            topicPartitionOffsets = loadOffsets(taskTopicPartitions);
            consumer.assign(topicPartitionOffsets.keySet());
        }

        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.entrySet().stream()
            .filter(x -> x.getValue() == 0L).count());
        log.trace("Seeking offsets: {}", topicPartitionOffsets);
        topicPartitionOffsets.forEach(consumer::seek);
        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        // nop
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetProducer, "offset producer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return "1";
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        try {
            if (stopping) {
                return;
            }
            if (!metadata.hasOffset()) {
                log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
                return;
            }
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            long latency = System.currentTimeMillis() - record.timestamp();
            metrics.countRecord(topicPartition);
            metrics.replicationLatency(topicPartition, latency);
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            maybeSyncOffsets(sourceTopicPartition, upstreamOffset, downstreamOffset);
        } catch (Throwable e) {
            log.warn("Failure committing record.", e);
        }
    }

    // updates partition state and sends OffsetSync if necessary
    private void maybeSyncOffsets(TopicPartition topicPartition, long upstreamOffset,
            long downstreamOffset) {
        PartitionState partitionState =
            partitionStates.computeIfAbsent(topicPartition, x -> new PartitionState(maxOffsetLag));
        if (partitionState.update(upstreamOffset, downstreamOffset)) {
            sendOffsetSync(topicPartition, upstreamOffset, downstreamOffset);
        }
    }

    // sends OffsetSync record upstream to internal offsets topic
    private void sendOffsetSync(TopicPartition topicPartition, long upstreamOffset,
            long downstreamOffset) {
        if (!outstandingOffsetSyncs.tryAcquire()) {
            // Too many outstanding offset syncs.
            return;
        }
        OffsetSync offsetSync = new OffsetSync(topicPartition, upstreamOffset, downstreamOffset);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(offsetSyncsTopic, 0,
                offsetSync.recordKey(), offsetSync.recordValue());
        offsetProducer.send(record, (x, e) -> {
            if (e != null) {
                log.error("Failure sending offset sync.", e);
            } else {
                log.trace("Sync'd offsets for {}: {}=={}", topicPartition,
                    upstreamOffset, downstreamOffset);
            }
            outstandingOffsetSyncs.release();
        });
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, x -> loadOffset(x)));
    }

    private Map<TopicPartition, Long> loadSyncOffsets(MirrorTaskConfig mirrorTaskConfig, Set<TopicPartition> topicPartitionSet) {
        log.info("==Load offset from topic mm2-offset-syncs");
        Map<TopicPartition, Long> topicPartitionLongMap = this.loadOffsets(mirrorTaskConfig, topicPartitionSet);
        log.info("==topicPartitionLongMap: {}", topicPartitionLongMap);
        this.consumer.assign(topicPartitionSet);
        return topicPartitionLongMap;
    }

    private Map<TopicPartition, Long> loadOffsets(MirrorTaskConfig config, Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        Map<TopicPartition, OffsetSync> sourceClusterTopicPartitionOffsetSyncMap =
                loadOffsetsFromCluster(config.sourceConsumerConfig(), config.offsetSyncsTopic(), this.pollTimeout, topicPartitions);
        Map<TopicPartition, OffsetSync> targetClusterTopicPartitionOffsetSyncMap =
                loadOffsetsFromCluster(config.targetConsumerConfig(), config.targetClusterOffsetSyncsTopic(), this.pollTimeout, topicPartitions);

        topicPartitions.forEach(topicPartition -> {
            OffsetSync sourceClusterOffset = sourceClusterTopicPartitionOffsetSyncMap.get(topicPartition);
            OffsetSync targetClusterOffset = targetClusterTopicPartitionOffsetSyncMap.get(topicPartition);

            if (sourceClusterOffset != null && targetClusterOffset != null) {
                long offset = Math.max(sourceClusterOffset.upstreamOffset(), targetClusterOffset.downstreamOffset());
                offsetMap.put(topicPartition, offset + 1L);
                log.info("==Topic partition: {} source offset: {}, target offset: {}", topicPartition, sourceClusterOffset.upstreamOffset(), targetClusterOffset.downstreamOffset());
            } else if(sourceClusterOffset != null) {
                offsetMap.put(topicPartition, sourceClusterOffset.upstreamOffset() + 1L);
                log.info("==Topic partition: {} source offset: {}", topicPartition, sourceClusterOffset.upstreamOffset());
            } else if (targetClusterOffset != null) {
                offsetMap.put(topicPartition, targetClusterOffset.downstreamOffset() + 1L);
                log.info("==Topic partition: {} target offset: {}", topicPartition, targetClusterOffset.downstreamOffset());
            } else {
                log.info("==Topic partition: {} offset is null, seek to beginning", topicPartition);
            }
        });
        return offsetMap;
    }

    private Map<TopicPartition, OffsetSync> loadOffsetsFromCluster(Map<String, Object> consumerConfig, String offsetSyncsTopic, Duration timeout, Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, OffsetSync> offsetSyncs = new HashMap<>();
        KafkaConsumer<byte[], byte[]> offsetSyncConsumer = new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        try{
            if (!offsetSyncConsumer.listTopics().containsKey(offsetSyncsTopic)) {
                return offsetSyncs;
            }

            TopicPartition offsetSyncTopicPartition = new TopicPartition(offsetSyncsTopic, 0);
            offsetSyncConsumer.assign(Collections.singleton(offsetSyncTopicPartition));
            offsetSyncConsumer.seekToBeginning(Collections.singleton(offsetSyncTopicPartition));

            while (!endOfStream(offsetSyncConsumer, Collections.singleton(offsetSyncTopicPartition))) {
                ConsumerRecords<byte[], byte[]> consumerRecords = offsetSyncConsumer.poll(timeout);
                consumerRecords.forEach(consumerRecord -> {
                    OffsetSync offsetSync = OffsetSync.deserializeRecord(consumerRecord);
                    offsetSyncs.put(offsetSync.topicPartition(), offsetSync);
                });
            }
        } finally {
            offsetSyncConsumer.close();
        }
        return offsetSyncs;
    }

    private static boolean endOfStream(Consumer<?,?> consumer, Collection<TopicPartition> assignments) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments);
        for (TopicPartition topicPartition : assignments) {
            if (consumer.position(topicPartition) < endOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset) + 1;
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    static class PartitionState {
        long previousUpstreamOffset = -1L;
        long previousDownstreamOffset = -1L;
        long lastSyncUpstreamOffset = -1L;
        long lastSyncDownstreamOffset = -1L;
        long maxOffsetLag;

        PartitionState(long maxOffsetLag) {
            this.maxOffsetLag = maxOffsetLag;
        }

        // true if we should emit an offset sync
        boolean update(long upstreamOffset, long downstreamOffset) {
            boolean shouldSyncOffsets = false;
            long upstreamStep = upstreamOffset - lastSyncUpstreamOffset;
            long downstreamTargetOffset = lastSyncDownstreamOffset + upstreamStep;
            if (lastSyncDownstreamOffset == -1L
                    || downstreamOffset - downstreamTargetOffset >= maxOffsetLag
                    || upstreamOffset - previousUpstreamOffset != 1L
                    || downstreamOffset < previousDownstreamOffset) {
                lastSyncUpstreamOffset = upstreamOffset;
                lastSyncDownstreamOffset = downstreamOffset;
                shouldSyncOffsets = true;
            }
            previousUpstreamOffset = upstreamOffset;
            previousDownstreamOffset = downstreamOffset;
            return shouldSyncOffsets;
        }
    }
}
