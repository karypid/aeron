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
package io.aeron.driver.media;

import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ReceiveChannelEndpointTest
{
    private final SystemCounters systemCounters = mock(SystemCounters.class);
    private final AtomicCounter statusIndicator = mock(AtomicCounter.class);
    private final UdpChannel udpChannel = mock(UdpChannel.class);
    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final DataPacketDispatcher dataPacketDispatcher = mock(DataPacketDispatcher.class);
    private final MediaDriver.Context context = new MediaDriver.Context()
        .systemCounters(systemCounters)
        .cachedNanoClock(nanoClock)
        .senderCachedNanoClock(nanoClock)
        .receiverCachedNanoClock(nanoClock)
        .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals())
        .senderPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, true))
        .receiverPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false));
    private ReceiveChannelEndpoint endpoint;

    @BeforeEach
    void before()
    {
        final AtomicCounter[] counters = new AtomicCounter[SystemCounterDescriptor.values().length];
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            counters[descriptor.id()] = mock(AtomicCounter.class);
        }
        when(systemCounters.get(any()))
            .thenAnswer(invocation ->
            {
                final SystemCounterDescriptor descriptor = invocation.getArgument(0);
                return counters[descriptor.id()];
            });

        endpoint = new ReceiveChannelEndpoint(udpChannel, dataPacketDispatcher, statusIndicator, context);
    }

    @Test
    void shouldRejectInvalidSetupMessage()
    {
        final SetupFlyweight setupFlyweight =
            new SetupFlyweight(new UnsafeBuffer(new byte[SetupFlyweight.HEADER_LENGTH]));
        setupFlyweight.mtuLength(111).termOffset(-5);

        endpoint.onSetupMessage(
            setupFlyweight,
            setupFlyweight,
            SetupFlyweight.HEADER_LENGTH,
            mock(InetSocketAddress.class),
            0);

        final AtomicCounter invalidPackets = systemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        verify(invalidPackets).increment();
        verifyNoMoreInteractions(invalidPackets);
        verifyNoInteractions(dataPacketDispatcher);
    }

    @Test
    void shouldAcceptValidSetupMessage()
    {
        final InetSocketAddress srcAddress = mock(InetSocketAddress.class);
        final int transportIndex = 7;
        final SetupFlyweight setupFlyweight =
            new SetupFlyweight(new UnsafeBuffer(new byte[SetupFlyweight.HEADER_LENGTH]));
        setupFlyweight.termOffset(0).mtuLength(2048).termLength(64 * 1024);

        endpoint.onSetupMessage(
            setupFlyweight,
            setupFlyweight,
            SetupFlyweight.HEADER_LENGTH,
            srcAddress,
            transportIndex);

        final AtomicCounter invalidPackets = systemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        verify(dataPacketDispatcher).onSetupMessage(endpoint, setupFlyweight, srcAddress, transportIndex);
        verifyNoMoreInteractions(invalidPackets);
        verifyNoInteractions(invalidPackets);
    }
}
