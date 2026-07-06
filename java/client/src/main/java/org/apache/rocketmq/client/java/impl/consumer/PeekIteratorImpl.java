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

package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.PeekMessageResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.Cursor;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.apis.consumer.PeekDirection;
import org.apache.rocketmq.client.apis.consumer.PeekIterator;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PeekIteratorImpl implements PeekIterator {
    private static final Logger log = LoggerFactory.getLogger(PeekIteratorImpl.class);
    private static final int PEEK_BATCH_SIZE = 3;

    private final LiteSubscriptionManager liteSubscriptionManager;
    private final String liteTopic;
    private final apache.rocketmq.v2.PeekDirection protoDirection;
    private final apache.rocketmq.v2.OffsetOption protoAnchor;

    private apache.rocketmq.v2.Cursor protoCursor = null;
    private List<MessageViewImpl> currentPage = Collections.emptyList();
    private int index = 0;
    private long restNum = Long.MAX_VALUE;

    /**
     * Per-broker [begin, end) tracking for consumed messages.
     */
    private final Map<String, long[]> consumedRanges = new HashMap<>();

    PeekIteratorImpl(LiteSubscriptionManager liteSubscriptionManager, String liteTopic,
        OffsetOption anchor, PeekDirection direction) {
        this.liteSubscriptionManager = liteSubscriptionManager;
        this.liteTopic = liteTopic;
        this.protoDirection = ProtobufUtils.toProtobufPeekDirection(direction);
        this.protoAnchor = ProtobufUtils.toProtobufOffsetOption(anchor);
    }

    @Override
    public OffsetOption toOffsetOption() {
        if (protoCursor == null || consumedRanges.isEmpty()) {
            throw new IllegalStateException("next() has not been called successfully yet");
        }
        Cursor.Builder builder = Cursor.newBuilder();
        for (Map.Entry<String, long[]> entry : consumedRanges.entrySet()) {
            builder.putRange(entry.getKey(),
                Cursor.OffsetRange.of(
                    entry.getValue()[0], entry.getValue()[1]));
        }
        return OffsetOption.ofCursor(builder.build());
    }

    @Override
    public boolean hasNext() {
        return index < currentPage.size() || restNum > 0;
    }

    @Override
    public MessageView next() throws ClientException {
        if (index >= currentPage.size()) {
            if (restNum <= 0) {
                throw new NoSuchElementException();
            }
            fetchNextBatch();
            // Fetch succeeded but yielded no messages.
            if (index >= currentPage.size()) {
                throw new NoSuchElementException();
            }
        }
        MessageViewImpl msg = currentPage.get(index++);
        updateConsumedRange(msg);
        return msg;
    }

    private void updateConsumedRange(MessageViewImpl msg) {
        String brokerName = msg.getBrokerName();
        if (brokerName == null) {
            log.warn("brokerName is null for message, group={}, topic={}, liteTopic={}, messageId={}",
                liteSubscriptionManager.getConsumerGroupName(), msg.getTopic(),
                liteTopic, msg.getMessageId());
            return;
        }
        long offset = msg.getOffset();
        long[] range = consumedRanges.get(brokerName);
        if (range == null) {
            consumedRanges.put(brokerName, new long[] {offset, offset + 1});
        } else {
            range[0] = Math.min(range[0], offset);
            range[1] = Math.max(range[1], offset + 1);
        }
    }

    private void fetchNextBatch() throws ClientException {
        apache.rocketmq.v2.OffsetOption effectiveOption = protoCursor != null
            ? apache.rocketmq.v2.OffsetOption.newBuilder().setCursor(protoCursor).build()
            : protoAnchor;
        PeekMessageResponse response =
            liteSubscriptionManager.peekInternal(liteTopic, PEEK_BATCH_SIZE, effectiveOption, protoDirection);
        // Update state only after the network call succeeds,
        // so that on failure the caller can retry next() safely.
        List<MessageViewImpl> messages = new ArrayList<>();
        for (Message message : response.getMessagesList()) {
            messages.add(MessageViewImpl.fromProtobuf(message));
        }
        restNum = response.getRestNum();
        protoCursor = response.getCursor();
        currentPage = messages;
        index = 0;
    }
}
