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

class StatusMessageFlyweightTest
{
    private final StatusMessageFlyweight flyweight =
        new StatusMessageFlyweight(new UnsafeBuffer(new byte[StatusMessageFlyweight.HEADER_LENGTH]));

    @ParameterizedTest
    @ValueSource(ints = { -128, -1, 1024 * 1024 * 1024, 2 * 1024 * 1024 })
    void shouldRejectFrameIfConsumptionTermOffsetIsOutOfBounds(final int termOffset)
    {
        flyweight.consumptionTermOffset(termOffset);

        assertFalse(flyweight.isValid(2 * 1024 * 1024));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 31 })
    void shouldRejectFrameIfConsumptionTermOffsetIsNotAligned(final int termOffset)
    {
        flyweight.consumptionTermOffset(termOffset);

        assertFalse(flyweight.isValid(2 * 1024 * 1024));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -4, 1024 * 1024 + 1, 4194304, 2 * 1024 * 1024, 2 * 1024 * 1024 - 1 })
    void shouldRejectFrameIfReceiveWindowIsOutOfBounds(final int receiverWindowLength)
    {
        flyweight.receiverWindowLength(receiverWindowLength);

        assertFalse(flyweight.isValid(2 * 1024 * 1024));
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "0,1024", "32768,32768", "65504,32768", "65504,10" })
    void shouldRecognizeValidFrame(final int termOffset, final int receiverWindowLength)
    {
        flyweight
            .streamId(ThreadLocalRandom.current().nextInt())
            .sessionId(ThreadLocalRandom.current().nextInt())
            .consumptionTermId(ThreadLocalRandom.current().nextInt())
            .consumptionTermOffset(termOffset)
            .receiverWindowLength(receiverWindowLength);

        assertTrue(flyweight.isValid(TERM_MIN_LENGTH));
    }
}
