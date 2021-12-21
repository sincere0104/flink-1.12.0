/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;
import java.util.Collection;

/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to an element.
 *
 * <p>In a window operation, elements are grouped by their key (if available) and by the windows to
 * which it was assigned. The set of elements with the same key and window is called a pane.
 * When a {@link Trigger} decides that a certain pane should fire the
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} is applied
 * to produce output elements for that pane.
 * TODO 对一个流进行window操作，元素以它们的key（keyBy函数指定）和它们所属的window进行分组。位于相同key和相同窗口的一组元素称之为窗格（pane）。
 *
 * @param <T> 元素类型 The type of elements that this WindowAssigner can assign windows to.
 * @param <W> 窗口类型 The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Returns a {@code Collection} of windows that should be assigned to the element.
	 * 获取包含元素的window集合
	 * @param element The element to which windows should be assigned. 进入窗口的元素
	 * @param timestamp The timestamp of the element. 元素的时间戳即 event time
	 * @param context The {@link WindowAssignerContext} in which the assigner operates.
	 *                WindowAssigner上下文对象，需要用于携带processing time。
	 */
	public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);

	/**
	 * Returns the default trigger associated with this {@code WindowAssigner}.
	 * 获取默认的触发器
	 */
	public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);

	/**
	 * Returns a {@link TypeSerializer} for serializing windows that are assigned by
	 * this {@code WindowAssigner}.
	 * 获取window的序列化器
	 */
	public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

	/**
	 * Returns {@code true} if elements are assigned to windows based on event time,
	 * {@code false} otherwise.
	 * 如果窗口是基于event time的，返回true。否则返回false
	 */
	public abstract boolean isEventTime();

	/**
	 * A context provided to the {@link WindowAssigner} that allows it to query the
	 * current processing time.
	 *
	 * <p>This is provided to the assigner by its containing
	 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator},
	 * which, in turn, gets it from the containing
	 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
	 */
	public abstract static class WindowAssignerContext {

		/**
		 * Returns the current processing time.
		 * 获取当前的processing time。在使用processing time类型的window时候会使用此方法。
		 */
		public abstract long getCurrentProcessingTime();

	}
}
