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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamOperator} for executing {@link KeyedProcessFunction KeyedProcessFunctions}.
 */
@Internal
public class KeyedProcessOperator<K, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, KeyedProcessFunction<K, IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;

	private transient ContextImpl context;

	private transient OnTimerContextImpl onTimerContext;

	public KeyedProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
		super(function);

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		//InternalTimerService的实例由getInternalTimerService()方法取得，该方法定义在所有算子的基类AbstractStreamOperator中
		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		//TimerService接口的实现类为SimpleTimerService，它实际上又是InternalTimerService的非常简单的代理
		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl(userFunction, timerService);
		onTimerContext = new OnTimerContextImpl(userFunction, timerService);
	}

	//当Timer触发时，实际上是根据时间特征调用onProcessingTime()/onEventTime()方法（这两个方法来自Triggerable接口），
	// 进而触发用户函数的onTimer()回调逻辑。
	@Override
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		invokeUserFunction(TimeDomain.EVENT_TIME, timer);
	}

	@Override
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.eraseTimestamp();
		//当timer触发时，调用用户自己的onTimer方法
		invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		context.element = element;
		//KeyedProcessOperator.processElement()方法调用用户自定义函数的processElement()方法，顺便将上下文实例ContextImpl传了进去，所以用户可以由它获得TimerService来注册Timer。
		userFunction.processElement(element.getValue(), context, collector);
		context.element = null;
	}

	private void invokeUserFunction(
			TimeDomain timeDomain,
			InternalTimer<K, VoidNamespace> timer) throws Exception {
		onTimerContext.timeDomain = timeDomain;
		onTimerContext.timer = timer;
		//调用 onTimer 方法
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	//其中以内部类的形式实现了KeyedProcessFunction需要的上下文类Context
	private class ContextImpl extends KeyedProcessFunction<K, IN, OUT>.Context {

		private final TimerService timerService;

		private StreamRecord<IN> element;

		ContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
			function.super();
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public Long timestamp() {
			checkState(element != null);

			if (element.hasTimestamp()) {
				return element.getTimestamp();
			} else {
				return null;
			}
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}

			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getCurrentKey() {
			return (K) KeyedProcessOperator.this.getCurrentKey();
		}
	}

	private class OnTimerContextImpl extends KeyedProcessFunction<K, IN, OUT>.OnTimerContext {

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<K, VoidNamespace> timer;

		OnTimerContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
			function.super();
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public Long timestamp() {
			checkState(timer != null);
			return timer.getTimestamp();
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}

			output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
		}

		@Override
		public K getCurrentKey() {
			return timer.getKey();
		}
	}
}
