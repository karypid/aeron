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
package io.aeron.logging;

import org.agrona.AsciiEncoding;
import org.agrona.DirectBuffer;

import java.io.IOException;

import static org.agrona.PrintBufferUtil.byteToHexStringPadded;

/**
 * Utility methods for loggers.
 */
public final class LogUtil
{
    private static final long NANOS_PER_SECOND = 1_000_000_000;

    private LogUtil()
    {
    }

    /**
     * Render a nanosecond timestamp to the supplied {@link StringBuilder} in the following format.
     * <pre>
     *     [&lt;seconds&gt;.&lt;nanoseconds&gt;]
     * </pre>
     *
     * @param builder       to render the timestamp too.
     * @param timestampNs   the nanosecond timestamp.
     */
    public static void appendTimestamp(final StringBuilder builder, final long timestampNs)
    {
        final long seconds = timestampNs / NANOS_PER_SECOND;
        final long nanos = timestampNs - seconds * NANOS_PER_SECOND;
        final int numDigitsAfterDot = AsciiEncoding.digitCount(nanos);
        builder.append('[');
        builder.append(seconds);
        builder.append('.');
        for (int i = 0, size = 9 - numDigitsAfterDot; i < size; i++)
        {
            builder.append('0');
        }
        builder.append(nanos);
        builder.append(']').append(' ');
    }

    /**
     * Render a nanosecond timestamp as a string in the following format.
     * <pre>
     *     [&lt;seconds&gt;.&lt;nanoseconds&gt;]
     * </pre>
     * Note: this will allocate a string builder internally, for a low allocation option use
     * {@link LogUtil#appendTimestamp(StringBuilder, long)}.
     *
     * @param timestampNs   the nanosecond timestamp.
     * @return the string formatted nanosecond timestamp.
     * @see #appendTimestamp(StringBuilder, long)
     */
    public static String renderTimestamp(final long timestampNs)
    {
        final StringBuilder sb = new StringBuilder();
        appendTimestamp(sb, timestampNs);
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Append an IPv6 address as a string.
     *
     * @param builder   to append to.
     * @param buffer    to read from.
     * @param offset    to read from.
     */
    public static void appendIpV6Address(final Appendable builder, final DirectBuffer buffer, final int offset)
    {
        int bestStart = -1;
        int bestLength = 0;
        int runStart = -1;
        int runLength = 0;

        for (int i = 0; i < 8; i++)
        {
            if (0 == ipV6Group(buffer, i, offset))
            {
                if (-1 == runStart)
                {
                    runStart = i;
                }
                runLength++;
            }
            else
            {
                if (runLength > bestLength)
                {
                    bestStart = runStart;
                    bestLength = runLength;
                }
                runStart = -1;
                runLength = 0;
            }
        }

        if (runLength > bestLength)
        {
            bestStart = runStart;
            bestLength = runLength;
        }

        if (bestLength < 2)
        {
            bestStart = -1;
        }

        try
        {
            builder.append('[');
            for (int i = 0; i < 8;)
            {
                if (i == bestStart)
                {
                    builder.append("::");
                    i += bestLength;
                    continue;
                }

                builder.append(byteToHexStringPadded(0xFF & buffer.getByte((i * 2 + offset))));
                builder.append(byteToHexStringPadded(0xFF & buffer.getByte((i * 2) + 1 + offset)));

                i++;

                if (i < 8 && i != bestStart)
                {
                    builder.append(':');
                }
            }
            builder.append(']');
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private static int ipV6Group(final DirectBuffer buffer, final int index, final int offset)
    {
        final int byteOffset = (index * 2) + offset;
        return ((buffer.getByte(byteOffset) << 8) & 0xFF00) | (buffer.getByte(byteOffset + 1) & 0xFF);
    }

    /**
     * Append a simple hex string to a {@link StringBuilder}.
     *
     * @param sb        to write the string to.
     * @param buffer    to read the data from.
     * @param offset    to read the data from.
     * @param length    to read the data from.
     */
    public static void appendHexString(
        final StringBuilder sb,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        for (int i = 0; i < length; i++)
        {
            final int index = offset + i;
            sb.append(byteToHexStringPadded(buffer.getByte(index)));
        }
    }
}
