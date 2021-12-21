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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that selects all the output channels.
 * 发送到所有的channel
 *
 * @param <T> Type of the elements in the Stream being broadcast
 */
@Internal
public class BroadcastPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Note: Broadcast mode could be handled directly for all the output channels
	 * in record writer, so it is no need to select channels via this method.
	 * Broadcast模式是直接发送到下游的所有task，所以不需要通过下面的方法选择发送的通道
	 */
	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		throw new UnsupportedOperationException("Broadcast partitioner does not support select channels.");
	}

	@Override
	public SubtaskStateMapper getUpstreamSubtaskStateMapper() {
		return SubtaskStateMapper.DISCARD_EXTRA_STATE;
	}

	@Override
	public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
		return SubtaskStateMapper.ROUND_ROBIN;
	}

	@Override
	public boolean isBroadcast() {
		return true;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "BROADCAST";
	}
}
