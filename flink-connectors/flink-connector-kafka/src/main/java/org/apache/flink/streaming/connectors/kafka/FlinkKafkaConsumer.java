/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions.
 *
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once".
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 *
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 *
 * <p>Please refer to Kafka's documentation for the available configuration properties:
 * http://kafka.apache.org/documentation.html#newconsumerconfigs</p>
 */
@PublicEvolving
public class FlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {

	private static final long serialVersionUID = 1L;

	/**  Configuration key to change the polling timeout.
	 *   配置轮询超时超时时间，使用flink.poll-timeout参数在properties进行配置
	 **/

	public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

	/** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
	 * available. If 0, returns immediately with any records that are available now.
	 * 如果没有可用数据，则等待轮询所需的时间（以毫秒为单位）。 如果为0，则立即返回所有可用的记录
	 * 默认轮询超时时间
	 * */
	public static final long DEFAULT_POLL_TIMEOUT = 100L;

	// ------------------------------------------------------------------------

	/** User-supplied properties for Kafka.  用户提供的kafka 参数配置**/
	protected final Properties properties;

	/** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
	 * available. If 0, returns immediately with any records that are available now
	 * 如果没有可用数据，则等待轮询所需的时间（以毫秒为单位）。 如果为0，则立即返回所有可用的记录
	 * */
	protected final long pollTimeout;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Kafka streaming source consumer.
	 *
	 * @param topic             The name of the topic that should be consumed. 消费的主题名称
	 * @param valueDeserializer The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 *                          反序列化类型，用于将kafka的字节消息转换为Flink的对象
	 * @param props             用户传入的kafka参数
	 */
	public FlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(Collections.singletonList(topic), valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer.
	 *
	 * <p>This constructor allows passing a {@see KafkaDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 * 该构造方法允许传入KafkaDeserializationSchema，该反序列化类支持访问kafka消费的额外信息
	 * 比如：key/value对，offsets(偏移量)，topic(主题名称)
	 *
	 * @param topic        The name of the topic that should be consumed.
	 * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 */
	public FlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(Collections.singletonList(topic), deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer.
	 *
	 * <p>This constructor allows passing multiple topics to the consumer.
	 * 创建一个kafka的consumer source
	 * 该构造方法允许传入多个topic(主题)，支持消费多个主题
	 *
	 * @param topics       The Kafka topics to read from.
	 * @param deserializer The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 */
	public FlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
		this(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer.
	 *
	 * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
	 * 创建一个kafka的consumer source
	 * 该构造方法允许传入多个topic(主题)，支持消费多个主题,
	 * 该构造方法允许传入KafkaDeserializationSchema，该反序列化类支持访问kafka消费的额外信息
	 * 比如：key/value对，offsets(偏移量)，topic(主题名称)
	 *
	 * @param topics       The Kafka topics to read from.
	 * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 */
	public FlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(topics, null, deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * 基于正则表达式订阅多个topic
	 * 如果开启了分区发现，即FlinkKafkaConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS值为非负数
	 * 只要是能够正则匹配上，主题一旦被创建就会立即被订阅
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * @param subscriptionPattern The regular expression for a pattern of topic names to subscribe to.
	 * @param valueDeserializer   The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 */
	public FlinkKafkaConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(null, subscriptionPattern, new KafkaDeserializationSchemaWrapper<>(valueDeserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * 基于正则表达式订阅多个topic
	 * 如果开启了分区发现，即FlinkKafkaConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS值为非负数
	 * 只要是能够正则匹配上，主题一旦被创建就会立即被订阅
	 *
	 * 该构造方法允许传入KafkaDeserializationSchema，该反序列化类支持访问kafka消费的额外信息
	 * 比如：key/value对，offsets(偏移量)，topic(主题名称)
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * <p>This constructor allows passing a {@see KafkaDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param subscriptionPattern The regular expression for a pattern of topic names to subscribe to.
	 * @param deserializer        The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 */
	public FlinkKafkaConsumer(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(null, subscriptionPattern, deserializer, props);
	}

	private FlinkKafkaConsumer(
		List<String> topics,
		Pattern subscriptionPattern,
		KafkaDeserializationSchema<T> deserializer,
		Properties props) {
		// 调用父类(FlinkKafkaConsumerBase)构造方法，PropertiesUtil.getLong方法第一个参数为Properties，第二个参数为key，第三个参数为value默认值
		super(
			topics,
			subscriptionPattern,
			deserializer,
			getLong(
				checkNotNull(props, "props"),
				KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_DISABLED),
			!getBoolean(props, KEY_DISABLE_METRICS, false));

		this.properties = props;
		setDeserializer(this.properties);

		// configure the polling timeout
		// 配置轮询超时时间，如果在properties中配置了KEY_POLL_TIMEOUT参数，则返回具体的配置值，否则返回默认值DEFAULT_POLL_TIMEOUT
		try {
			if (properties.containsKey(KEY_POLL_TIMEOUT)) {
				this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
			} else {
				this.pollTimeout = DEFAULT_POLL_TIMEOUT;
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
		}
	}

	/**
	 *父类(FlinkKafkaConsumerBase)方法重写，该方法的作用是返回一个fetcher实例，
     *fetcher的作用是连接kafka的broker，拉去数据并进行反序列化，然后将数据输出为数据流(data stream)
	 **/
	@Override
	protected AbstractFetcher<T, ?> createFetcher(
		SourceContext<T> sourceContext,
		Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		StreamingRuntimeContext runtimeContext,
		OffsetCommitMode offsetCommitMode,
		MetricGroup consumerMetricGroup,
		boolean useMetrics) throws Exception {

		// make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
		// this overwrites whatever setting the user configured in the properties
		// 确保当偏移量的提交模式为ON_CHECKPOINTS(条件1：开启checkpoint，条件2：consumer.setCommitOffsetsOnCheckpoints(true))时，禁用自动提交
		// 该方法为父类(FlinkKafkaConsumerBase)的静态方法
		// 这将覆盖用户在properties中配置的任何设置
		// 当offset的模式为ON_CHECKPOINTS，或者为DISABLED时，会将用户配置的properties属性进行覆盖
		// 具体是将ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"的值重置为"false
		// 可以理解为：如果开启了checkpoint，并且设置了consumer.setCommitOffsetsOnCheckpoints(true)，默认为true，
		// 就会将kafka properties的enable.auto.commit强制置为false
		adjustAutoCommitConfig(properties, offsetCommitMode);

		return new KafkaFetcher<>(
			sourceContext,
			assignedPartitionsWithInitialOffsets,
			watermarkStrategy,
			runtimeContext.getProcessingTimeService(),
			runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
			runtimeContext.getUserCodeClassLoader(),
			runtimeContext.getTaskNameWithSubtasks(),
			deserializer,
			properties,
			pollTimeout,
			runtimeContext.getMetricGroup(),
			consumerMetricGroup,
			useMetrics);
	}

	/**
	 *
	 * @param topicsDescriptor Descriptor that describes whether we are discovering partitions for fixed topics or a topic pattern.
	 * @param indexOfThisSubtask The index of this consumer subtask.
	 * @param numParallelSubtasks The total number of parallel consumer subtasks.
	 *
	 * 父类(FlinkKafkaConsumerBase)方法重写
	 * 返回一个分区发现类，分区发现可以使用kafka broker的高级consumer API发现topic和partition的元数据
	 *
	 * @return
	 */
	@Override
	protected AbstractPartitionDiscoverer createPartitionDiscoverer(
		KafkaTopicsDescriptor topicsDescriptor,
		int indexOfThisSubtask,
		int numParallelSubtasks) {

		return new KafkaPartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties);
	}

	/**
	 * 根据时间戳获取每个分区的offset
	 * @param partitions
	 * @param timestamp
	 * @return
	 */
	@Override
	protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
		Collection<KafkaTopicPartition> partitions,
		long timestamp) {

		Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
		for (KafkaTopicPartition partition : partitions) {
			partitionOffsetsRequest.put(
				new TopicPartition(partition.getTopic(), partition.getPartition()),
				timestamp);
		}

		final Map<KafkaTopicPartition, Long> result = new HashMap<>(partitions.size());
		// use a short-lived consumer to fetch the offsets;
		// this is ok because this is a one-time operation that happens only on startup
		try (KafkaConsumer<?, ?> consumer = new KafkaConsumer(properties)) {
			for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
				consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

				result.put(
					new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
					(partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
			}

		}
		return result;
	}

	/**
	 *判断是否在kafka的参数开启了自动提交，即enable.auto.commit=true，
	 * 并且auto.commit.interval.ms>0,
	 * 注意：如果没有没有设置enable.auto.commit的参数，则默认为true
	 *       如果没有设置auto.commit.interval.ms的参数，则默认为5000毫秒
	 * @return
	 */
	@Override
	protected boolean getIsAutoCommitEnabled() {
		return getBoolean(properties, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true) &&
			PropertiesUtil.getLong(properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000) > 0;
	}

	/**
	 * Makes sure that the ByteArrayDeserializer is registered in the Kafka properties.
	 * 确保配置了kafka消息的key与value的反序列化方式，
	 * 如果没有配置，则使用ByteArrayDeserializer序列化器，
	 * 该类的deserialize方法是直接将数据进行return，未做任何处理
	 *
	 * @param props The Kafka properties to register the serializer in.
	 */
	private static void setDeserializer(Properties props) {
		final String deSerName = ByteArrayDeserializer.class.getName();

		Object keyDeSer = props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
		Object valDeSer = props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

		if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured key DeSerializer ({})", ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
		}
		if (valDeSer != null && !valDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured value DeSerializer ({})", ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
		}

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deSerName);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deSerName);
	}
}
