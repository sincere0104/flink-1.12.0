/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is the following: When the source should start emitting elements, the {@link #run} method
 * is called with a {@link SourceContext} that can be used for emitting elements.
 * The run method can run for as long as necessary. The source must, however, react to an
 * invocation of {@link #cancel()} by breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * <p>Sources that also implement the {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}
 * interface must ensure that state checkpointing, updating of internal state and emission of
 * elements are not done concurrently. This is achieved by using the provided checkpointing lock
 * object to protect update of state and emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count = count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 *
 * <h3>Timestamps and watermarks:</h3>
 * Sources may assign timestamps to elements and may manually emit watermarks.
 * However, these are only interpreted if the streaming program runs on
 * {@link TimeCharacteristic#EventTime}. On other time characteristics
 * ({@link TimeCharacteristic#IngestionTime} and {@link TimeCharacteristic#ProcessingTime}),
 * the watermarks from the source function are ignored.
 *
 * @param <T> The type of the elements produced by this source.
 *
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

	/**
	 * Starts the source. Implementations can use the {@link SourceContext} emit
	 * elements.
	 *
	 * <p>Sources that implement {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}
	 * must lock on the checkpoint lock (using a synchronized block) before updating internal
	 * state and emitting elements, to make both an atomic operation:
	 *
	 * <pre>{@code
	 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
	 *      private long count = 0L;
	 *      private volatile boolean isRunning = true;
	 *
	 *      private transient ListState<Long> checkpointedCount;
	 *
	 *      public void run(SourceContext<T> ctx) {
	 *          while (isRunning && count < 1000) {
	 *              // this synchronized block ensures that state checkpointing,
	 *              // internal state updates and emission of elements are an atomic operation
	 *              synchronized (ctx.getCheckpointLock()) {
	 *                  ctx.collect(count);
	 *                  count++;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void cancel() {
	 *          isRunning = false;
	 *      }
	 *
	 *      public void initializeState(FunctionInitializationContext context) {
	 *          this.checkpointedCount = context
	 *              .getOperatorStateStore()
	 *              .getListState(new ListStateDescriptor<>("count", Long.class));
	 *
	 *          if (context.isRestored()) {
	 *              for (Long count : this.checkpointedCount.get()) {
	 *                  this.count = count;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void snapshotState(FunctionSnapshotContext context) {
	 *          this.checkpointedCount.clear();
	 *          this.checkpointedCount.add(count);
	 *      }
	 * }
	 * }</pre>
	 *
	 * @param ctx The context to emit elements to and for accessing locks.
	 * TODO
	 * 不断调用循环发送数据。
	 * 使用一个状态变量控制循环的执行。当cancel方法执行后必须能够跳出循环，停止发送数据。
	 * 使用SourceContext的collect等方法将元素发送至下游。
	 * 如果使用Checkpoint，在SourceContext collect数据的时候必须加锁。防止checkpoint操作和发送数据操作同时进行。
	 */
	void run(SourceContext<T> ctx) throws Exception;

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run(SourceContext)} method. The implementation needs to ensure that the
	 * source will break out of that loop after this method is called.
	 *
	 * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
	 * {@code false} in this method. That flag is checked in the loop condition.
	 *
	 * <p>When a source is canceled, the executing thread will also be interrupted
	 * (via {@link Thread#interrupt()}). The interruption happens strictly after this
	 * method has been called, so any interruption handler can rely on the fact that
	 * this method has completed. It is good practice to make any flags altered by
	 * this method "volatile", in order to guarantee the visibility of the effects of
	 * this method to any interruption handler.
	 *
	 * TODO cancel方法在数据源停止的时候调用。cancel方法必须能够控制run方法中的循环，停止循环的运行。并做一些状态清理操作。
	 */
	void cancel();

	// ------------------------------------------------------------------------
	//  source context
	// ------------------------------------------------------------------------

	/**
	 * Interface that source functions use to emit elements, and possibly watermarks.
	 * TODO SourceContext在SourceFunction中使用，用于向下游发送数据，或者是发送watermark
	 * @param <T> The type of the elements produced by the source.
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface SourceContext<T> {

		/**
		 * Emits one element from the source, without attaching a timestamp. In most cases,
		 * this is the default way of emitting elements.
		 * TODO 向下游发送数据
		 * 如果使用ProcessingTime，该元素不携带timestamp。
		 * 如果使用IngestionTime，元素使用系统当前时间作为timestamp。
		 * 如果使用EventTime，元素不携带timestamp。需要在数据流后续为元素指定timestamp（assignTimestampAndWatermark）
		 * <p>The timestamp that the element will get assigned depends on the time characteristic of
		 * the streaming program:
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's
		 *         current time as the timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp initially.
		 *         It needs to get a timestamp (via a {@link TimestampAssigner}) before any time-dependent
		 *         operation (like time windows).</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 */
		void collect(T element);

		/**
		 * Emits one element from the source, and attaches the given timestamp. This method
		 * is relevant for programs using {@link TimeCharacteristic#EventTime}, where the
		 * sources assign timestamps themselves, rather than relying on a {@link TimestampAssigner}
		 * on the stream.
		 *
		 * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
		 * This allows programs to switch between the different time characteristics and behaviors
		 * without changing the code of the source functions.
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
		 *         because processing time never works with element timestamps.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
		 *         system's current time, to realize proper ingestion time semantics.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.</li>
		 * </ul>
		 * TODO  向下游发送带有timestamp的数据。和collect方法一样也有如下三种情况
		 * 如果使用ProcessingTime，timestamp会被忽略
		 * 如果使用IngestionTime，使用系统时间覆盖timestamp
		 * 如果使用EventTime，使用指定的timestamp
		 *
		 * @param element The element to emit
		 * @param timestamp The timestamp in milliseconds since the Epoch
		 */
		@PublicEvolving
		void collectWithTimestamp(T element, long timestamp);

		/**
		 * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
		 * elements with a timestamp {@code t' <= t} will occur any more. If further such
		 * elements will be emitted, those elements are considered <i>late</i>.
		 *
		 * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}.
		 * On {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On
		 * {@link TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the
		 * automatic ingestion time watermarks.
		 *
		 * @param mark The Watermark to emit
		 * TODO
		 * 向下游发送watermark。watermark也包含一个timestamp。
		 * 向下游发送watermark意味着所有在watermark的timestamp之前的数据已经到齐。
		 * 如果在watermark之后，收到了timestamp比该watermark的timestamp小的元素，
		 * 该元素会被认为迟到，将会被系统忽略，或者进入到旁路输出（side output）。
		 */
		@PublicEvolving
		void emitWatermark(Watermark mark);

		/**
		 * Marks the source to be temporarily idle. This tells the system that this source will
		 * temporarily stop emitting records and watermarks for an indefinite amount of time. This
		 * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and
		 * {@link TimeCharacteristic#EventTime}, allowing downstream tasks to advance their
		 * watermarks without the need to wait for watermarks from this source while it is idle.
		 * TODO 标记此数据源暂时闲置
		 * 该数据源暂时不会发送任何数据和watermark。
		 * 仅对 IngestionTime 和 EventTime 生效。
		 * 下游任务前移 watermark 的时候将不会再等待被标记为闲置的数据源的 watermark。
		 * <p>Source functions should make a best effort to call this method as soon as they
		 * acknowledge themselves to be idle. The system will consider the source to resume activity
		 * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T, long)},
		 * or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
		 */
		@PublicEvolving
		void markAsTemporarilyIdle();

		/**
		 * Returns the checkpoint lock. Please refer to the class-level comment in
		 * {@link SourceFunction} for details about how to write a consistent checkpointed
		 * source.
		 * 返回检查节点锁
		 *
		 * @return The object to use as the lock
		 */
		Object getCheckpointLock();

		/**
		 * This method is called by the system to shut down the context.
		 * 系统调用此方法来关闭上下文
		 */
		void close();
	}
}
