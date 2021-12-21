/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * A {@code WindowAssigner} that can merge windows.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class MergingWindowAssigner<T, W extends Window> extends WindowAssigner<T, W> {
	private static final long serialVersionUID = 1L;

	/**
	 * Determines which windows (if any) should be merged.
	 *
	 * @param windows The window candidates.
	 * @param callback A callback that can be invoked to signal which windows should be merged.
	 */
	// 合并窗口的逻辑
	// 参数为：windows: 待合并的窗口集合，callback: 回调函数，用于通知将要被合并的window
	public abstract void mergeWindows(Collection<W> windows, MergeCallback<W> callback);

	/**
	 * Callback to be used in {@link #mergeWindows(Collection, MergeCallback)} for specifying which
	 * windows should be merged.
	 */
	public interface MergeCallback<W> {

		/**
		 * Specifies that the given windows should be merged into the result window.
		 *
		 * @param toBeMerged The list of windows that should be merged into one window.
		 * @param mergeResult The resulting merged window.
		 */
		// toBeMerged：将要被合并的window
		// mergeResult：合并结果窗口
		void merge(Collection<W> toBeMerged, W mergeResult);
	}
}
