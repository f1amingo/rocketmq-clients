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

import java.util.Map;
import org.junit.Test;

public class CursorTest {

    // ========== OffsetRange tests ==========

    @Test
    public void testOffsetRangeOf() {
        Cursor.OffsetRange range = Cursor.OffsetRange.of(10L, 20L);
        assertThat(range.getBegin()).isEqualTo(10L);
        assertThat(range.getEnd()).isEqualTo(20L);
    }

    @Test
    public void testOffsetRangeEquals() {
        Cursor.OffsetRange range1 = Cursor.OffsetRange.of(0L, 5L);
        Cursor.OffsetRange range2 = Cursor.OffsetRange.of(0L, 5L);
        Cursor.OffsetRange range3 = Cursor.OffsetRange.of(0L, 10L);

        assertThat(range1).isEqualTo(range2);
        assertThat(range1).isNotEqualTo(range3);
        assertThat(range1).isNotEqualTo(null);
        assertThat(range1).isNotEqualTo("not a range");
    }

    @Test
    public void testOffsetRangeEqualsSameInstance() {
        Cursor.OffsetRange range = Cursor.OffsetRange.of(1L, 2L);
        assertThat(range).isEqualTo(range);
    }

    @Test
    public void testOffsetRangeHashCode() {
        Cursor.OffsetRange range1 = Cursor.OffsetRange.of(0L, 5L);
        Cursor.OffsetRange range2 = Cursor.OffsetRange.of(0L, 5L);
        assertThat(range1.hashCode()).isEqualTo(range2.hashCode());
    }

    @Test
    public void testOffsetRangeToString() {
        Cursor.OffsetRange range = Cursor.OffsetRange.of(3L, 7L);
        assertThat(range.toString()).contains("begin=3").contains("end=7");
    }

    @Test
    public void testOffsetRangeEmptyInterval() {
        // start == end is a valid empty interval
        Cursor.OffsetRange range = Cursor.OffsetRange.of(5L, 5L);
        assertThat(range.getBegin()).isEqualTo(5L);
        assertThat(range.getEnd()).isEqualTo(5L);
    }

    // ========== Cursor builder tests ==========

    @Test
    public void testBuilderEmpty() {
        Cursor cursor = Cursor.newBuilder().build();
        assertThat(cursor.getRanges()).isEmpty();
    }

    @Test
    public void testBuilderSingleRange() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();

        assertThat(cursor.getRanges()).hasSize(1);
        assertThat(cursor.getRanges().get("broker-0")).isEqualTo(Cursor.OffsetRange.of(0L, 10L));
    }

    @Test
    public void testBuilderMultipleRanges() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .putRange("broker-1", Cursor.OffsetRange.of(10L, 20L))
            .build();

        assertThat(cursor.getRanges()).hasSize(2);
        assertThat(cursor.getRanges().get("broker-0")).isEqualTo(Cursor.OffsetRange.of(0L, 5L));
        assertThat(cursor.getRanges().get("broker-1")).isEqualTo(Cursor.OffsetRange.of(10L, 20L));
    }

    @Test
    public void testBuilderOverwriteSameBroker() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .putRange("broker-0", Cursor.OffsetRange.of(5L, 10L))
            .build();

        assertThat(cursor.getRanges()).hasSize(1);
        assertThat(cursor.getRanges().get("broker-0")).isEqualTo(Cursor.OffsetRange.of(5L, 10L));
    }

    // ========== Cursor immutability tests ==========

    @Test
    public void testRangesMapIsUnmodifiable() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();

        Map<String, Cursor.OffsetRange> ranges = cursor.getRanges();
        assertThatThrownBy(() -> ranges.put("broker-1", Cursor.OffsetRange.of(0L, 1L)))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testBuilderDoesNotShareStateWithCursor() {
        Cursor.Builder builder = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L));
        Cursor cursor = builder.build();

        // Modifying builder after build should not affect the built cursor
        builder.putRange("broker-1", Cursor.OffsetRange.of(10L, 20L));

        assertThat(cursor.getRanges()).hasSize(1);
    }

    // ========== Cursor equals/hashCode tests ==========

    @Test
    public void testCursorEquals() {
        Cursor cursor1 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        Cursor cursor2 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        Cursor cursor3 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 10L))
            .build();

        assertThat(cursor1).isEqualTo(cursor2);
        assertThat(cursor1).isNotEqualTo(cursor3);
        assertThat(cursor1).isNotEqualTo(null);
        assertThat(cursor1).isNotEqualTo("not a cursor");
    }

    @Test
    public void testCursorEqualsSameInstance() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        assertThat(cursor).isEqualTo(cursor);
    }

    @Test
    public void testCursorHashCodeConsistentWithEquals() {
        Cursor cursor1 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        Cursor cursor2 = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        assertThat(cursor1.hashCode()).isEqualTo(cursor2.hashCode());
    }

    @Test
    public void testCursorToString() {
        Cursor cursor = Cursor.newBuilder()
            .putRange("broker-0", Cursor.OffsetRange.of(0L, 5L))
            .build();
        assertThat(cursor.toString()).contains("Cursor").contains("broker-0");
    }
}
