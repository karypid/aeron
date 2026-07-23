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

import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NakFlyweightTest
{
    private final NakFlyweight flyweight = new NakFlyweight(new UnsafeBuffer(new byte[NakFlyweight.HEADER_LENGTH]));

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1024, 128 * 1024 * 1024, TERM_MIN_LENGTH })
    void shouldRejectFrameIfTermOffsetIsOutOfBounds(final int termOffset)
    {
        flyweight.termOffset(termOffset);

        assertFalse(flyweight.isValid(TERM_MIN_LENGTH));
    }

    @ParameterizedTest
    @ValueSource(ints = { 100, 31 })
    void shouldRejectFrameIfTermOffsetIsNotAligned(final int termOffset)
    {
        flyweight.termOffset(termOffset);

        assertFalse(flyweight.isValid(TERM_MIN_LENGTH));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1 })
    void shouldRejectFrameIfLengthIsNegative(final int length)
    {
        flyweight.termOffset(0).length(length);

        assertFalse(flyweight.isValid(TERM_MIN_LENGTH));
    }

    @ParameterizedTest
    @CsvSource({ "0,2097152", "1048544,512" })
    void shouldRejectFrameIfLengthPlusTermOffsetExceedTermBoundaries(final int termOffset, final int length)
    {
        flyweight.termOffset(termOffset).length(length);

        assertFalse(flyweight.isValid(1048576));
    }

    @ParameterizedTest
    @CsvSource({ "0,2097152", "1048544,512", "0,0", "128,0" })
    void shouldRecognizeValidFrames(final int termOffset, final int length)
    {
        flyweight
            .sessionId(ThreadLocalRandom.current().nextInt())
            .streamId(ThreadLocalRandom.current().nextInt())
            .termId(ThreadLocalRandom.current().nextInt())
            .termOffset(termOffset)
            .length(length);

        assertTrue(flyweight.isValid(2097152));
    }
}
