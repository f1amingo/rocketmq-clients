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

package org.apache.rocketmq.client.java.misc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.rocketmq.client.apis.consumer.Cursor;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.apis.consumer.PeekDirection;
import org.junit.Test;

public class ProtobufUtilsTest {
    @Test
    public void testToProtobufOffsetOptionWithPolicy() {
        OffsetOption offsetOption = OffsetOption.LAST_OFFSET;
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasPolicy()).isTrue();
        assertThat(protobufOffsetOption.getPolicy()).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.LAST);
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithOffset() {
        long offsetValue = 100L;
        OffsetOption offsetOption = OffsetOption.ofOffset(offsetValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasOffset()).isTrue();
        assertThat(protobufOffsetOption.getOffset()).isEqualTo(offsetValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithTailN() {
        long tailNValue = 5L;
        OffsetOption offsetOption = OffsetOption.ofTailN(tailNValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasTailN()).isTrue();
        assertThat(protobufOffsetOption.getTailN()).isEqualTo(tailNValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithTimestamp() {
        long timestampValue = System.currentTimeMillis();
        OffsetOption offsetOption = OffsetOption.ofTimestamp(timestampValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasTimestamp()).isTrue();
        assertThat(protobufOffsetOption.getTimestamp()).isEqualTo(timestampValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
    }

    @Test
    public void testToProtobufPolicyWithLast() {
        long policyValue = OffsetOption.POLICY_LAST_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.LAST);
    }

    @Test
    public void testToProtobufPolicyWithMin() {
        long policyValue = OffsetOption.POLICY_MIN_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MIN);
    }

    @Test
    public void testToProtobufPolicyWithMax() {
        long policyValue = OffsetOption.POLICY_MAX_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MAX);
    }

    @Test
    public void testToProtobufPolicyWithUnknownValue() {
        long unknownPolicyValue = 999L;
        assertThatThrownBy(() -> ProtobufUtils.toProtobufPolicy(unknownPolicyValue))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown policy type");
    }

    // ========== toProtobufCursor tests ==========

    @Test
    public void testToProtobufCursorEmpty() {
        Cursor cursor = Cursor.newBuilder().build();
        apache.rocketmq.v2.Cursor protoCursor = ProtobufUtils.toProtobufCursor(cursor);
        assertThat(protoCursor.getRangesMap()).isEmpty();
    }

    @Test
    public void testToProtobufCursorSingleRange() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        apache.rocketmq.v2.Cursor protoCursor = ProtobufUtils.toProtobufCursor(cursor);

        assertThat(protoCursor.getRangesMap()).hasSize(1);
        assertThat(protoCursor.getRangesMap()).containsKey("broker-0");
        apache.rocketmq.v2.Cursor.OffsetRange protoRange = protoCursor.getRangesMap().get("broker-0");
        assertThat(protoRange.getBegin()).isEqualTo(0L);
        assertThat(protoRange.getEnd()).isEqualTo(10L);
    }

    @Test
    public void testToProtobufCursorMultipleRanges() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .putRange("broker-1", Cursor.OffsetRange.of(10L, 20L))
            .build();
        apache.rocketmq.v2.Cursor protoCursor = ProtobufUtils.toProtobufCursor(cursor);

        assertThat(protoCursor.getRangesMap()).hasSize(2);
        assertThat(protoCursor.getRangesMap().get("broker-0").getEnd()).isEqualTo(5L);
        assertThat(protoCursor.getRangesMap().get("broker-1").getBegin()).isEqualTo(10L);
    }

    // ========== toProtobufPeekDirection tests ==========

    @Test
    public void testToProtobufPeekDirectionForward() {
        apache.rocketmq.v2.PeekDirection result = ProtobufUtils.toProtobufPeekDirection(PeekDirection.FORWARD);
        assertThat(result).isEqualTo(apache.rocketmq.v2.PeekDirection.FORWARD);
    }

    @Test
    public void testToProtobufPeekDirectionBackward() {
        apache.rocketmq.v2.PeekDirection result = ProtobufUtils.toProtobufPeekDirection(PeekDirection.BACKWARD);
        assertThat(result).isEqualTo(apache.rocketmq.v2.PeekDirection.BACKWARD);
    }

    // ========== toProtobufOffsetOption with CURSOR type ==========

    @Test
    public void testToProtobufOffsetOptionWithCursor() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        OffsetOption offsetOption = OffsetOption.ofCursor(cursor);
        apache.rocketmq.v2.OffsetOption protoOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);

        assertThat(protoOption.hasCursor()).isTrue();
        assertThat(protoOption.getCursor().getRangesMap()).hasSize(1);
        assertThat(protoOption.getCursor().getRangesMap().get("broker-0").getBegin()).isEqualTo(0L);
        assertThat(protoOption.getCursor().getRangesMap().get("broker-0").getEnd()).isEqualTo(10L);
        assertThat(protoOption.hasPolicy()).isFalse();
        assertThat(protoOption.hasOffset()).isFalse();
        assertThat(protoOption.hasTailN()).isFalse();
        assertThat(protoOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithMinPolicy() {
        OffsetOption offsetOption = OffsetOption.MIN_OFFSET;
        apache.rocketmq.v2.OffsetOption protoOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protoOption.hasPolicy()).isTrue();
        assertThat(protoOption.getPolicy()).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MIN);
    }

    @Test
    public void testToProtobufOffsetOptionWithMaxPolicy() {
        OffsetOption offsetOption = OffsetOption.MAX_OFFSET;
        apache.rocketmq.v2.OffsetOption protoOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protoOption.hasPolicy()).isTrue();
        assertThat(protoOption.getPolicy()).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MAX);
    }
}