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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.protocol.ResolutionEntryFlyweight.MAX_NAME_LENGTH;
import static io.aeron.protocol.ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH;
import static io.aeron.protocol.ResolutionEntryFlyweight.MIN_IPV6_FRAME_LENGTH;
import static io.aeron.protocol.ResolutionEntryFlyweight.NAME_FIELD_OFFSET_IP4;
import static io.aeron.protocol.ResolutionEntryFlyweight.NAME_FIELD_OFFSET_IP6;
import static io.aeron.protocol.ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP4_MD;
import static io.aeron.protocol.ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP6_MD;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResolutionEntryFlyweightTest
{
    @ParameterizedTest
    @ValueSource(ints = { 0, 1, MIN_IPV4_FRAME_LENGTH - 1 })
    void shouldMarkAsInvalidIfInsufficientCapacity(final int capacity)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[capacity]));
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(bytes = { -1, 5, 127, -128 })
    void shouldMarkAsInvalidIfResTypeIsInvalid(final byte resType)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV4_FRAME_LENGTH]));
        entry.resType(resType);
        assertFalse(entry.isValid());
    }

    @Test
    void shouldMarkAsInvalidIfCapacityIsTooSmallIPv6()
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV6_FRAME_LENGTH - 1]));
        entry.resType(RES_TYPE_NAME_TO_IP6_MD);
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(shorts = { Short.MIN_VALUE, -3, 0 })
    void shouldMarkAsInvalidIfNameLengthIsInvalidIPv4(final short nameLength)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV4_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP4, nameLength, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(shorts = { Short.MAX_VALUE, MAX_NAME_LENGTH + 1 })
    void shouldMarkAsInvalidIfNameLengthIsTooLongIPv4(final short nameLength)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV4_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP4, nameLength, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @Test
    void shouldMarkAsInvalidIfNameLengthIsBeyondBufferCapacityIPv4()
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV4_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP4, (short)5, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(shorts = { Short.MIN_VALUE, -3, 0 })
    void shouldMarkAsInvalidIfNameLengthIsInvalidIPv6(final short nameLength)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV6_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP6, nameLength, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(shorts = { Short.MAX_VALUE, MAX_NAME_LENGTH + 1 })
    void shouldMarkAsInvalidIfNameLengthIsTooLongIPv6(final short nameLength)
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV6_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP6, nameLength, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @Test
    void shouldMarkAsInvalidIfNameLengthIsBeyondBufferCapacityIPv6()
    {
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV6_FRAME_LENGTH]));
        entry.putShort(NAME_FIELD_OFFSET_IP6, (short)5, LITTLE_ENDIAN);
        assertFalse(entry.isValid());
    }

    @ParameterizedTest
    @ValueSource(bytes = { RES_TYPE_NAME_TO_IP4_MD, RES_TYPE_NAME_TO_IP6_MD })
    void shouldRecognizeValidEntry(final byte resType)
    {
        final String name = "test name";
        final ResolutionEntryFlyweight entry =
            new ResolutionEntryFlyweight(new UnsafeBuffer(new byte[MIN_IPV6_FRAME_LENGTH + name.length()]));
        final byte[] nameBytes = name.getBytes(US_ASCII);
        entry.resType(resType).putName(nameBytes);

        assertTrue(entry.isValid());
        final byte[] actualBytes = new byte[nameBytes.length];
        entry.getName(actualBytes);
        assertArrayEquals(nameBytes, actualBytes);
    }
}
