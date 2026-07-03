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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A snapshot of per-broker read progress across a lite topic.
 * <p>Each entry maps a broker name to an {@link OffsetRange} representing
 * the [begin, end) interval of messages that have been peeked.
 */
public final class Cursor {

    private final Map<String, OffsetRange> ranges;

    private Cursor(Map<String, OffsetRange> ranges) {
        this.ranges = Collections.unmodifiableMap(new LinkedHashMap<>(ranges));
    }

    public Map<String, OffsetRange> getRanges() {
        return ranges;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Cursor cursor = (Cursor) o;
        return Objects.equals(ranges, cursor.ranges);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ranges);
    }

    @Override
    public String toString() {
        return "Cursor{ranges=" + ranges + '}';
    }

    /**
     * Represents a half-open [begin, end) offset range for a single broker.
     */
    public static final class OffsetRange {

        private final long begin;
        private final long end;

        private OffsetRange(long begin, long end) {
            this.begin = begin;
            this.end = end;
        }

        public static OffsetRange of(long begin, long end) {
            return new OffsetRange(begin, end);
        }

        public long getBegin() {
            return begin;
        }

        public long getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OffsetRange that = (OffsetRange) o;
            return begin == that.begin && end == that.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(begin, end);
        }

        @Override
        public String toString() {
            return "OffsetRange{begin=" + begin + ", end=" + end + '}';
        }
    }

    /**
     * Builder for {@link Cursor}.
     */
    public static final class Builder {

        private final Map<String, OffsetRange> ranges = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder putRange(String brokerName, OffsetRange range) {
            ranges.put(brokerName, range);
            return this;
        }

        public Cursor build() {
            return new Cursor(ranges);
        }
    }
}
