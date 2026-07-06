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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class OffsetOptionTest {

    // ========== Policy constants tests ==========

    @Test
    public void testPolicyConstants() {
        assertThat(OffsetOption.POLICY_LAST_VALUE).isEqualTo(0L);
        assertThat(OffsetOption.POLICY_MIN_VALUE).isEqualTo(1L);
        assertThat(OffsetOption.POLICY_MAX_VALUE).isEqualTo(2L);
    }

    @Test
    public void testPredefinedPolicyInstances() {
        assertThat(OffsetOption.LAST_OFFSET.getType()).isEqualTo(OffsetOption.Type.POLICY);
        assertThat(OffsetOption.LAST_OFFSET.getValue()).isEqualTo(OffsetOption.POLICY_LAST_VALUE);

        assertThat(OffsetOption.MIN_OFFSET.getType()).isEqualTo(OffsetOption.Type.POLICY);
        assertThat(OffsetOption.MIN_OFFSET.getValue()).isEqualTo(OffsetOption.POLICY_MIN_VALUE);

        assertThat(OffsetOption.MAX_OFFSET.getType()).isEqualTo(OffsetOption.Type.POLICY);
        assertThat(OffsetOption.MAX_OFFSET.getValue()).isEqualTo(OffsetOption.POLICY_MAX_VALUE);
    }

    // ========== ofOffset tests ==========

    @Test
    public void testOfOffset() {
        OffsetOption option = OffsetOption.ofOffset(100L);
        assertThat(option.getType()).isEqualTo(OffsetOption.Type.OFFSET);
        assertThat(option.getValue()).isEqualTo(100L);
        assertThat(option.getCursor()).isNull();
    }

    @Test
    public void testOfOffsetZero() {
        OffsetOption option = OffsetOption.ofOffset(0L);
        assertThat(option.getType()).isEqualTo(OffsetOption.Type.OFFSET);
        assertThat(option.getValue()).isEqualTo(0L);
    }

    @Test
    public void testOfOffsetNegativeThrows() {
        assertThatThrownBy(() -> OffsetOption.ofOffset(-1L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("offset must be greater than or equal to 0");
    }

    // ========== ofTailN tests ==========

    @Test
    public void testOfTailN() {
        OffsetOption option = OffsetOption.ofTailN(5L);
        assertThat(option.getType()).isEqualTo(OffsetOption.Type.TAIL_N);
        assertThat(option.getValue()).isEqualTo(5L);
    }

    @Test
    public void testOfTailNNegativeThrows() {
        assertThatThrownBy(() -> OffsetOption.ofTailN(-1L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("tailN must be greater than or equal to 0");
    }

    // ========== ofTimestamp tests ==========

    @Test
    public void testOfTimestamp() {
        long ts = System.currentTimeMillis();
        OffsetOption option = OffsetOption.ofTimestamp(ts);
        assertThat(option.getType()).isEqualTo(OffsetOption.Type.TIMESTAMP);
        assertThat(option.getValue()).isEqualTo(ts);
    }

    @Test
    public void testOfTimestampNegativeThrows() {
        assertThatThrownBy(() -> OffsetOption.ofTimestamp(-1L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("timestamp must be greater than or equal to 0");
    }

    // ========== ofCursor tests (new) ==========

    @Test
    public void testOfCursor() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        OffsetOption option = OffsetOption.ofCursor(cursor);

        assertThat(option.getType()).isEqualTo(OffsetOption.Type.CURSOR);
        assertThat(option.getCursor()).isEqualTo(cursor);
        assertThat(option.getValue()).isEqualTo(0L);
    }

    @Test
    public void testOfCursorNullThrows() {
        assertThatThrownBy(() -> OffsetOption.ofCursor(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cursor must not be null");
    }

    @Test
    public void testOfCursorEmptyCursor() {
        Cursor emptyCursor = Cursor.newBuilder().build();
        OffsetOption option = OffsetOption.ofCursor(emptyCursor);

        assertThat(option.getType()).isEqualTo(OffsetOption.Type.CURSOR);
        assertThat(option.getCursor()).isEqualTo(emptyCursor);
        assertThat(option.getCursor().getRanges()).isEmpty();
    }

    // ========== equals/hashCode tests ==========

    @Test
    public void testEqualsOffsetOptions() {
        OffsetOption option1 = OffsetOption.ofOffset(100L);
        OffsetOption option2 = OffsetOption.ofOffset(100L);
        OffsetOption option3 = OffsetOption.ofOffset(200L);

        assertThat(option1).isEqualTo(option2);
        assertThat(option1).isNotEqualTo(option3);
    }

    @Test
    public void testEqualsCursorOptions() {
        Cursor cursor1 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        Cursor cursor2 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();

        OffsetOption option1 = OffsetOption.ofCursor(cursor1);
        OffsetOption option2 = OffsetOption.ofCursor(cursor2);

        assertThat(option1).isEqualTo(option2);
    }

    @Test
    public void testNotEqualsDifferentTypes() {
        OffsetOption offsetOption = OffsetOption.ofOffset(0L);
        OffsetOption policyOption = OffsetOption.LAST_OFFSET;

        assertThat(offsetOption).isNotEqualTo(policyOption);
    }

    @Test
    public void testEqualsNull() {
        OffsetOption option = OffsetOption.ofOffset(100L);
        assertThat(option).isNotEqualTo(null);
    }

    @Test
    public void testHashCodeConsistentWithEquals() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        OffsetOption option1 = OffsetOption.ofCursor(cursor);
        OffsetOption option2 = OffsetOption.ofCursor(cursor);

        assertThat(option1.hashCode()).isEqualTo(option2.hashCode());
    }

    // ========== toString tests ==========

    @Test
    public void testToStringPolicy() {
        String str = OffsetOption.LAST_OFFSET.toString();
        assertThat(str).contains("POLICY");
    }

    @Test
    public void testToStringOffset() {
        String str = OffsetOption.ofOffset(42L).toString();
        assertThat(str).contains("OFFSET").contains("42");
    }

    @Test
    public void testToStringCursor() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();
        String str = OffsetOption.ofCursor(cursor).toString();
        assertThat(str).contains("CURSOR").contains("broker-0");
    }
}
