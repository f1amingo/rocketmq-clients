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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.apis.consumer.PeekDirection;
import org.apache.rocketmq.client.apis.consumer.PeekIterator;
import org.apache.rocketmq.client.apis.message.MessageView;

class PeekIteratorImpl implements PeekIterator {
    private static final int PEEK_BATCH_SIZE = 3;

    private final LiteSubscriptionManager liteSubscriptionManager;
    private final String liteTopic;
    private final PeekDirection direction;
    private final OffsetOption anchor;

    private String cursor = null;
    private Iterator<MessageView> currentPage = Collections.emptyIterator();
    private long restNum = Long.MAX_VALUE;

    PeekIteratorImpl(LiteSubscriptionManager liteSubscriptionManager, String liteTopic,
        OffsetOption anchor, PeekDirection direction) {
        this.liteSubscriptionManager = liteSubscriptionManager;
        this.liteTopic = liteTopic;
        this.direction = direction;
        this.anchor = anchor;
    }

    @Override
    public boolean hasNext() {
        return currentPage.hasNext() || restNum > 0;
    }

    @Override
    public MessageView next() {
        if (currentPage.hasNext()) {
            return currentPage.next();
        }
        if (restNum <= 0) {
            throw new NoSuchElementException();
        }
        fetchNextBatch();
        // Current page was empty and fetch succeeded but yielded no messages.
        if (!currentPage.hasNext()) {
            throw new NoSuchElementException();
        }
        return currentPage.next();
    }

    private void fetchNextBatch() {
        try {
            LiteSubscriptionManager.PeekResult result =
                liteSubscriptionManager.peekInternal(
                    liteTopic, PEEK_BATCH_SIZE, anchor, cursor, direction);
            // Update state only after the network call succeeds,
            // so that on failure the caller can retry next() safely.
            restNum = result.restNum;
            cursor = result.cursor;
            currentPage = result.messages.iterator();
        } catch (ClientException e) {
            throw new RuntimeException("Failed to fetch next batch in peek iterator", e);
        }
    }
}
