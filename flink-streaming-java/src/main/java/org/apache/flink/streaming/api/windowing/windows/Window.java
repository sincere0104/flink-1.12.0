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

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@code Window} is a grouping of elements into finite buckets. Windows have a maximum timestamp
 * which means that, at some point, all elements that go into one window will have arrived.
 * TODO Window 是将元素分组到有限的存储桶中。窗口有一个最大时间戳，这意味着，在某个时刻，进入一个窗口的所有元素都将到达。
 *
 * <p>Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 *
 * TODO 子类应该实现{@code equals（）}和{@code hashCode（）}，以便逻辑上相同的窗口被视为相同的窗口。
 */
@PublicEvolving
public abstract class Window {

	/**
	 * Gets the largest timestamp that still belongs to this window.
	 * TODO  获取属于此窗口的最大时间戳
	 *
	 * @return The largest timestamp that still belongs to this window.
	 */
	public abstract long maxTimestamp();
}
