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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.PeekMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.apis.consumer.PeekDirection;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.misc.ProtobufUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeekIteratorImplTest {

    private static final String FAKE_TOPIC = "test-topic";
    private static final String FAKE_LITE_TOPIC = "test-lite-topic";
    private static final String FAKE_HOST = "127.0.0.1";

    @Mock
    private LiteSubscriptionManager liteSubscriptionManager;

    @Before
    public void setUp() {
    }

    // ========== hasNext() tests ==========

    @Test
    public void testHasNextInitialStateReturnsTrue() {
        // restNum defaults to Long.MAX_VALUE, so hasNext should be true initially
        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        assertThat(iterator.hasNext()).isTrue();
    }

    @Test
    public void testHasNextAfterExhaustedReturnsFalse() throws ClientException {
        // First fetch returns messages with restNum=0
        PeekMessageResponse response = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // Trigger fetch by calling next
        // No messages and restNum=0, so next should throw
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testHasNextWithRestMessagesReturnsTrue() throws ClientException {
        PeekMessageResponse response = buildPeekResponseWithMessages(3, 5);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // Consume all 3 messages from first batch
        iterator.next();
        iterator.next();
        iterator.next();

        // restNum=5 so hasNext should still be true
        assertThat(iterator.hasNext()).isTrue();
    }

    // ========== next() tests ==========

    @Test
    public void testNextSingleBatch() throws ClientException {
        PeekMessageResponse response = buildPeekResponseWithMessages(3, 0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        int count = 0;
        while (iterator.hasNext()) {
            MessageView msg = iterator.next();
            assertThat(msg).isNotNull();
            count++;
        }
        assertThat(count).isEqualTo(3);
    }

    @Test
    public void testNextMultipleBatches() throws ClientException {
        // First batch: 3 messages, restNum=2
        PeekMessageResponse firstResponse = buildPeekResponseWithMessages(3, 2);
        // Second batch: 2 messages, restNum=0
        PeekMessageResponse secondResponse = buildPeekResponseWithMessages(2, 0);

        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(firstResponse)
            .thenReturn(secondResponse);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count).isEqualTo(5);
        verify(liteSubscriptionManager, times(2)).peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any());
    }

    @Test
    public void testNextNoMessagesThrowsNoSuchElement() throws ClientException {
        PeekMessageResponse emptyResponse = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(emptyResponse);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testNextEmptyBatchThenHasMoreThrowsNoSuchElement() throws ClientException {
        // First fetch returns 0 messages but restNum > 0
        // The response has no messages but restNum=5
        PeekMessageResponse emptyWithRest = PeekMessageResponse.newBuilder()
            .setRestNum(5)
            .build();

        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(emptyWithRest);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // hasNext is true (restNum=5 > 0), but fetch yields no messages
        assertThat(iterator.hasNext()).isTrue();
        // next triggers fetch, gets 0 messages → NoSuchElementException
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testNextClientExceptionPropagates() throws ClientException {
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenThrow(new ClientException("peek failed"));

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        assertThatThrownBy(iterator::next)
            .isInstanceOf(ClientException.class)
            .hasMessageContaining("peek failed");
    }

    @Test
    public void testNextAfterExceptionCanRetry() throws ClientException {
        // First call fails, second succeeds
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenThrow(new ClientException("transient error"))
            .thenReturn(buildPeekResponseWithMessages(2, 0));

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // First call fails
        assertThatThrownBy(iterator::next)
            .isInstanceOf(ClientException.class);

        // Retry should succeed - state was not corrupted
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    // ========== Cursor-based pagination tests ==========

    @Test
    public void testFetchNextBatchUsesCursorAfterFirstFetch() throws ClientException {
        apache.rocketmq.v2.Cursor protoCursor = apache.rocketmq.v2.Cursor.newBuilder()
            .putRanges("broker-0",
                apache.rocketmq.v2.Cursor.OffsetRange.newBuilder()
                    .setBegin(0).setEnd(3).build())
            .build();

        PeekMessageResponse firstResponse = buildPeekResponseWithMessages(3, 2);
        PeekMessageResponse firstWithCursor = PeekMessageResponse.newBuilder()
            .addAllMessages(firstResponse.getMessagesList())
            .setRestNum(firstResponse.getRestNum())
            .setCursor(protoCursor)
            .build();

        PeekMessageResponse secondResponse = buildPeekResponseWithMessages(2, 0);

        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(firstWithCursor)
            .thenReturn(secondResponse);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // Consume all messages to trigger both fetches
        while (iterator.hasNext()) {
            iterator.next();
        }

        // Verify second call used cursor-based OffsetOption
        verify(liteSubscriptionManager, times(2)).peekInternal(
            eq(FAKE_LITE_TOPIC), eq(3), any(), any());
    }

    @Test
    public void testFetchNextBatchFirstCallUsesAnchor() throws ClientException {
        PeekMessageResponse response = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        OffsetOption anchor = OffsetOption.MIN_OFFSET;
        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            anchor, PeekDirection.FORWARD);

        // Trigger fetch
        try {
            iterator.next();
        } catch (NoSuchElementException e) {
            // expected: empty response with restNum=0
        }

        // Verify the anchor was converted and passed
        apache.rocketmq.v2.OffsetOption expectedOption =
            ProtobufUtils.toProtobufOffsetOption(anchor);
        verify(liteSubscriptionManager).peekInternal(
            eq(FAKE_LITE_TOPIC), eq(3), eq(expectedOption),
            eq(apache.rocketmq.v2.PeekDirection.FORWARD));
    }

    // ========== toOffsetOption() tests ==========

    @Test
    public void testToOffsetOptionBeforeNextThrowsIllegalState() {
        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        assertThatThrownBy(iterator::toOffsetOption)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("next() has not been called successfully yet");
    }

    @Test
    public void testToOffsetOptionWithBrokerNameFromSystemProperties() throws ClientException {
        // Messages have broker_name in SystemProperties → consumedRanges populated correctly
        String brokerName = "broker-a";
        PeekMessageResponse response = buildPeekResponseWithBrokerName(3, 0, brokerName);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // Consume all 3 messages (offsets 0, 1, 2)
        while (iterator.hasNext()) {
            iterator.next();
        }

        // toOffsetOption should succeed with range [0, 3) keyed by brokerName
        org.apache.rocketmq.client.apis.consumer.OffsetOption option = iterator.toOffsetOption();
        assertThat(option.getType()).isEqualTo(org.apache.rocketmq.client.apis.consumer.OffsetOption.Type.CURSOR);
        org.apache.rocketmq.client.apis.consumer.Cursor cursor = option.getCursor();
        assertThat(cursor.getRanges()).containsKey(brokerName);
        org.apache.rocketmq.client.apis.consumer.Cursor.OffsetRange range =
            cursor.getRanges().get(brokerName);
        assertThat(range.getBegin()).isEqualTo(0);
        assertThat(range.getEnd()).isEqualTo(3);
    }

    // ========== Direction tests ==========

    @Test
    public void testBackwardDirection() throws ClientException {
        PeekMessageResponse response = buildPeekResponseWithMessages(2, 0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(),
            eq(apache.rocketmq.v2.PeekDirection.BACKWARD)))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MAX_OFFSET, PeekDirection.BACKWARD);

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    // ========== OffsetOption types tests ==========

    @Test
    public void testWithMaxOffsetAnchor() throws ClientException {
        PeekMessageResponse response = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MAX_OFFSET, PeekDirection.BACKWARD);

        try {
            iterator.next();
        } catch (NoSuchElementException e) {
            // expected: empty response with restNum=0
        }

        apache.rocketmq.v2.OffsetOption expectedOption =
            ProtobufUtils.toProtobufOffsetOption(OffsetOption.MAX_OFFSET);
        verify(liteSubscriptionManager).peekInternal(
            eq(FAKE_LITE_TOPIC), eq(3), eq(expectedOption),
            eq(apache.rocketmq.v2.PeekDirection.BACKWARD));
    }

    @Test
    public void testWithTimestampAnchor() throws ClientException {
        long timestamp = System.currentTimeMillis();
        OffsetOption anchor = OffsetOption.ofTimestamp(timestamp);
        PeekMessageResponse response = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            anchor, PeekDirection.FORWARD);

        try {
            iterator.next();
        } catch (NoSuchElementException e) {
            // expected: empty response with restNum=0
        }

        apache.rocketmq.v2.OffsetOption expectedOption =
            ProtobufUtils.toProtobufOffsetOption(anchor);
        verify(liteSubscriptionManager).peekInternal(
            eq(FAKE_LITE_TOPIC), eq(3), eq(expectedOption),
            eq(apache.rocketmq.v2.PeekDirection.FORWARD));
    }

    // ========== Batch size tests ==========

    @Test
    public void testBatchSizeIsThree() throws ClientException {
        PeekMessageResponse response = buildPeekResponse(0);
        when(liteSubscriptionManager.peekInternal(
            anyString(), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        try {
            iterator.next();
        } catch (NoSuchElementException e) {
            // expected: empty response with restNum=0
        }

        verify(liteSubscriptionManager).peekInternal(
            eq(FAKE_LITE_TOPIC), eq(3), any(), any());
    }

    // ========== Edge case tests ==========

    @Test
    public void testNextExhaustedThenNoMore() throws ClientException {
        PeekMessageResponse response = buildPeekResponseWithMessages(1, 0);
        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(response);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        // Consume the one message
        assertThat(iterator.hasNext()).isTrue();
        iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        // Calling next when exhausted throws NoSuchElementException
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testMultipleFetchesThreeBatches() throws ClientException {
        PeekMessageResponse first = buildPeekResponseWithMessages(3, 4);
        PeekMessageResponse second = buildPeekResponseWithMessages(3, 1);
        PeekMessageResponse third = buildPeekResponseWithMessages(1, 0);

        when(liteSubscriptionManager.peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any()))
            .thenReturn(first)
            .thenReturn(second)
            .thenReturn(third);

        PeekIteratorImpl iterator = new PeekIteratorImpl(
            liteSubscriptionManager, FAKE_LITE_TOPIC,
            OffsetOption.MIN_OFFSET, PeekDirection.FORWARD);

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count).isEqualTo(7);
        verify(liteSubscriptionManager, times(3)).peekInternal(
            eq(FAKE_LITE_TOPIC), anyInt(), any(), any());
    }

    // ========== Helper methods ==========

    private PeekMessageResponse buildPeekResponse(long restNum) {
        return PeekMessageResponse.newBuilder()
            .setRestNum(restNum)
            .build();
    }

    private PeekMessageResponse buildPeekResponseWithMessages(int msgCount, long restNum) {
        PeekMessageResponse.Builder builder = PeekMessageResponse.newBuilder()
            .setRestNum(restNum);
        for (int i = 0; i < msgCount; i++) {
            builder.addMessages(buildPbMessageWithOffset(i));
        }
        return builder.build();
    }

    private Message buildPbMessageWithOffset(long offset) {
        return buildPbMessageWithOffsetAndBrokerName(offset, null);
    }

    private PeekMessageResponse buildPeekResponseWithBrokerName(int msgCount, long restNum, String brokerName) {
        PeekMessageResponse.Builder builder = PeekMessageResponse.newBuilder()
            .setRestNum(restNum);
        for (int i = 0; i < msgCount; i++) {
            builder.addMessages(buildPbMessageWithOffsetAndBrokerName(i, brokerName));
        }
        return builder.build();
    }

    private Message buildPbMessageWithOffsetAndBrokerName(long offset, String brokerName) {
        Digest digest = Digest.newBuilder()
            .setType(DigestType.CRC32)
            .setChecksum("9EF61F95")
            .build();
        SystemProperties.Builder spBuilder = SystemProperties.newBuilder()
            .setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST)
            .setBodyDigest(digest)
            .setQueueOffset(offset)
            .setReceiptHandle("handle-" + offset);
        if (brokerName != null) {
            spBuilder.setBrokerName(brokerName);
        }
        SystemProperties systemProperties = spBuilder.build();
        Resource topicResource = Resource.newBuilder()
            .setName(FAKE_TOPIC)
            .build();
        return Message.newBuilder()
            .setSystemProperties(systemProperties)
            .setTopic(topicResource)
            .setBody(ByteString.copyFrom("body", StandardCharsets.UTF_8))
            .build();
    }
}
