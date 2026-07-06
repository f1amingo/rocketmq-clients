/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.apis.consumer;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * Iterator for peeking messages from a lite topic without affecting the consumer's commit offset.
 * <p>Use {@link #hasNext()} and {@link #next()} to iterate messages.
 * After consuming one or more messages, call {@link #toOffsetOption()} to capture the position
 * of the last returned message, which can later be used to resume consumption.
 */
public interface PeekIterator {

    /**
     * Returns {@code true} if the iteration has more messages.
     *
     * @return {@code true} if more messages are available.
     */
    boolean hasNext();

    /**
     * Returns the next message in the iteration.
     *
     * @return the next {@link MessageView}.
     * @throws java.util.NoSuchElementException if the iteration has no more messages;
     *                                          check with {@link #hasNext()} first.
     * @throws ClientException                  if a network or server error occurs; callers may
     *                                          retry by calling this method again.
     */
    MessageView next() throws ClientException;

    /**
     * Returns an {@link OffsetOption} representing the position of the last message
     * returned by {@link #next()}, which can be passed to
     * {@link LitePushConsumer#subscribeLite(String, OffsetOption)} or
     * {@link LiteSimpleConsumer#subscribeLite(String, OffsetOption)}
     * to resume consumption from that point.
     *
     * @return the {@link OffsetOption} for resuming consumption.
     * @throws IllegalStateException if {@link #next()} has never been called successfully.
     */
    OffsetOption toOffsetOption();
}
