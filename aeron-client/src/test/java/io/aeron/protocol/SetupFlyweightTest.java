/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.protocol;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.protocol.SetupFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.SetupFlyweight.MAX_UDP_PAYLOAD_LENGTH;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SetupFlyweightTest
{
    private final SetupFlyweight flyweight = new SetupFlyweight(new UnsafeBuffer(new byte[HEADER_LENGTH]));

    @ParameterizedTest
    @ValueSource(ints = { 0, 8, TERM_MIN_LENGTH / 2, TERM_MAX_LENGTH + 1 })
    void shouldRejectFrameIfTermLengthIsOutOfBounds(final int termLength)
    {
        flyweight
            .mtuLength(64)
            .termOffset(0)
            .termLength(termLength);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { TERM_MIN_LENGTH - 1, TERM_MAX_LENGTH - 1, 1_000_000 })
    void shouldRejectFrameIfTermLengthIsNotPowerOfTwo(final int termLength)
    {
        flyweight
            .mtuLength(64)
            .termOffset(0)
            .termLength(termLength);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { -32, TERM_MIN_LENGTH, TERM_MIN_LENGTH * 2 })
    void shouldRejectFrameIfTermOffsetIsOutOfBounds(final int termOffset)
    {
        flyweight
            .mtuLength(64)
            .termOffset(termOffset)
            .termLength(TERM_MIN_LENGTH);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, TERM_MIN_LENGTH - 55 })
    void shouldRejectFrameIfTermOffsetIsNotAligned(final int termOffset)
    {
        flyweight
            .mtuLength(64)
            .termOffset(termOffset)
            .termLength(TERM_MIN_LENGTH);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, DataHeaderFlyweight.HEADER_LENGTH - 1, MAX_UDP_PAYLOAD_LENGTH + FRAME_ALIGNMENT })
    void shouldRejectFrameIfMtuIsOutOfBounds(final int mtu)
    {
        flyweight
            .mtuLength(mtu)
            .termOffset(0)
            .termLength(TERM_MIN_LENGTH);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { DataHeaderFlyweight.HEADER_LENGTH + 1, 999 })
    void shouldRejectFrameIfMtuIsNotAligned(final int mtu)
    {
        flyweight
            .mtuLength(mtu)
            .termOffset(0)
            .termLength(TERM_MIN_LENGTH);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @CsvSource({ "32,0,65536", "1408,0,1073741824", "128,1073741792,1073741824", "4096,64,1048576" })
    void shouldRejectFrameIfMtuIsNotAligned(final int mtu, final int termOffset, final int termLength)
    {
        flyweight
            .sessionId(ThreadLocalRandom.current().nextInt())
            .streamId(ThreadLocalRandom.current().nextInt())
            .initialTermId(ThreadLocalRandom.current().nextInt())
            .activeTermId(ThreadLocalRandom.current().nextInt())
            .mtuLength(mtu)
            .termOffset(termOffset)
            .termLength(termLength);

        assertTrue(flyweight.isValid());
    }
}
