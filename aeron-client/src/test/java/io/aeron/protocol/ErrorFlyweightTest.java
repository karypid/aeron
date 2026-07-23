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

import io.aeron.test.Tests;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.protocol.ErrorFlyweight.ERROR_STRING_FIELD_OFFSET;
import static io.aeron.protocol.ErrorFlyweight.HAS_GROUP_ID_FLAG;
import static io.aeron.protocol.ErrorFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.ErrorFlyweight.MAX_ERROR_FRAME_LENGTH;
import static io.aeron.protocol.ErrorFlyweight.MAX_ERROR_MESSAGE_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorFlyweightTest
{
    private final ErrorFlyweight errorFlyweight = new ErrorFlyweight(allocate(MAX_ERROR_FRAME_LENGTH));

    @Test
    void shouldCorrectlySetFlagsForGroupTag()
    {
        assertEquals(0, errorFlyweight.flags());
        errorFlyweight.groupTag(10L);
        assertEquals(HAS_GROUP_ID_FLAG, errorFlyweight.flags());

        errorFlyweight.groupTag(null);
        assertNotEquals(HAS_GROUP_ID_FLAG, errorFlyweight.flags());
    }

    @Test
    void shouldCorrectlyUpdateExistingFlagsForGroupTag()
    {
        final short initialFlags = (short)0x3;

        errorFlyweight.flags(initialFlags);
        assertEquals(initialFlags, errorFlyweight.flags());
        errorFlyweight.groupTag(10L);
        assertEquals(HAS_GROUP_ID_FLAG, HAS_GROUP_ID_FLAG & errorFlyweight.flags());
        assertEquals(initialFlags, initialFlags & errorFlyweight.flags());

        errorFlyweight.groupTag(null);
        assertEquals(0, HAS_GROUP_ID_FLAG & errorFlyweight.flags());
        assertEquals(initialFlags, errorFlyweight.flags());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldStoreEmptyErrorMessageAsZeroLengthBytes(final String errorMessage)
    {
        errorFlyweight.putInt(ERROR_STRING_FIELD_OFFSET, 1024, LITTLE_ENDIAN);

        errorFlyweight.errorMessage(errorMessage);

        assertEquals(0, errorFlyweight.getInt(ERROR_STRING_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals("", errorFlyweight.errorMessage());
        assertEquals(HEADER_LENGTH, errorFlyweight.frameLength());
    }

    @ParameterizedTest
    @ValueSource(strings = { "abc", "xyzDEFEFJLF" })
    void shouldStoreErrorMessageAsIs(final String errorMessage)
    {
        errorFlyweight.putInt(ERROR_STRING_FIELD_OFFSET, -5, LITTLE_ENDIAN);

        errorFlyweight.errorMessage(errorMessage);

        assertEquals(errorMessage.length(), errorFlyweight.getInt(ERROR_STRING_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(errorMessage, errorFlyweight.errorMessage());
    }

    @Test
    void shouldTruncateErrorMessageIfTooLong()
    {
        final String errorMessage = Tests.generateStringWithSuffix("error-", "X", 2048);

        errorFlyweight.errorMessage(errorMessage);

        assertEquals(errorMessage.substring(0, MAX_ERROR_MESSAGE_LENGTH), errorFlyweight.errorMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -100, 3000, MAX_ERROR_MESSAGE_LENGTH + 1 })
    void shouldRejectFrameIfErrorStringLengthIsInvalid(final int length)
    {
        errorFlyweight.putInt(ERROR_STRING_FIELD_OFFSET, length, LITTLE_ENDIAN);

        assertFalse(errorFlyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 100 })
    void shouldRejectFrameIfErrorStringLengthIsOutBounds(final int length)
    {
        final ErrorFlyweight flyweight = new ErrorFlyweight(new UnsafeBuffer(new byte[HEADER_LENGTH]));
        flyweight.frameLength(HEADER_LENGTH);
        flyweight.putInt(ERROR_STRING_FIELD_OFFSET, length, LITTLE_ENDIAN);

        assertFalse(flyweight.isValid());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 5, 99, 100 })
    void shouldRecognizeValidFrame(final int length)
    {
        final ErrorFlyweight flyweight = new ErrorFlyweight(new UnsafeBuffer(new byte[HEADER_LENGTH + 100]));
        flyweight.frameLength(length + HEADER_LENGTH);
        flyweight.putInt(ERROR_STRING_FIELD_OFFSET, length, LITTLE_ENDIAN);

        assertTrue(flyweight.isValid());
    }

    @Test
    void shouldAllowMaxMessageSize()
    {
        final String errorMessage = Tests.generateStringWithSuffix("", "x", MAX_ERROR_MESSAGE_LENGTH);
        assertEquals(MAX_ERROR_MESSAGE_LENGTH, errorMessage.length());

        errorFlyweight.errorMessage(errorMessage);
        assertEquals(errorMessage, errorFlyweight.errorMessage());
        assertEquals(MAX_ERROR_FRAME_LENGTH, errorFlyweight.frameLength());

        assertTrue(errorFlyweight.isValid());
    }
}
