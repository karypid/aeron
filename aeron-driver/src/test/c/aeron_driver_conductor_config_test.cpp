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

#include "aeron_driver_conductor_test.h"

using testing::_;
using testing::HasSubstr;

class DriverConductorConfigTest : public DriverConductorTest, public testing::Test
{
};

TEST_F(DriverConductorConfigTest, shouldRejectResponsePublicationDestination)
{
    addPublication(1234, 1234, "aeron:udp?control=localhost:4000|control-mode=manual", 1234, false);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|control-mode=response");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
    .WillOnce(
        [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
        {
            const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
            char *msg_buf = (char *)malloc(msg->error_message_length + 1);

            memset(msg_buf, 0, msg->error_message_length + 1);
            memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

            EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_KEY));
            EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE));

            free(msg_buf);
        });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|response-correlation-id=2345");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_URI_RESPONSE_CORRELATION_ID_KEY));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorConfigTest, shouldRejectResponseSubscriptionDestination)
{
    addSubscription(1234, 1234, "aeron:udp?control-mode=manual", 1234);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addReceiveDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|control-mode=response");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_KEY));
                EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addReceiveDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|response-correlation-id=2345");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_URI_RESPONSE_CORRELATION_ID_KEY));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

class DriverConductorConfigTestParams : public DriverConductorTest, public testing::TestWithParam<std::string>
{
};

INSTANTIATE_TEST_SUITE_P(
    DriverConductorConfigTestParams,
    DriverConductorConfigTestParams,
    testing::Values(
    "aeron:udp?endpoint=localhost:24325",
    "aeron:ipc"));

TEST_P(DriverConductorConfigTestParams, shouldReturnStorageSpaceErrorIfNotEnoughStorageSpaceAvailable)
{
    m_context.m_context->usable_fs_space_func = [](const char* path) -> uint64_t
    {
        return 2049;
    };
    m_context.m_context->perform_storage_checks = true;

    std::string channel = GetParam();
    ASSERT_EQ(0, addPublication(333, 555, channel, 1000, true));

    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_EQ(AERON_ERROR_CODE_STORAGE_SPACE, msg->error_code);
                auto expected_error_text = std::string("insufficient usable storage for new log");
                EXPECT_NE(std::string::npos, std::string(msg_buf).find(expected_error_text));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);
}

TEST_P(DriverConductorConfigTestParams, shouldWarnIfRemainingStorageSpaceIsLow)
{
    m_context.m_context->usable_fs_space_func = [](const char *path) -> uint64_t
    {
        return 1000000;
    };
    m_context.m_context->low_file_store_warning_threshold = 2020202020ULL;
    m_context.m_context->perform_storage_checks = true;

    std::string channel = GetParam();
    ASSERT_EQ(0, addPublication(1, 17, channel, 9, false));

    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    auto errors_list = m_context.m_context->error_log->observation_list;
    EXPECT_NE(nullptr, errors_list);
    EXPECT_NE(0, errors_list->num_observations);
    auto last_error = errors_list->observations[errors_list->num_observations - 1];
    EXPECT_EQ(-AERON_ERROR_CODE_STORAGE_SPACE, last_error.error_code);
    auto error_text = std::string(last_error.description);
    EXPECT_EQ(error_text.size(), last_error.description_length);
    auto expected_warning =
        std::string("WARNING: space is running low: threshold=2020202020 usable=1000000 in ")
            .append(m_context.m_context->aeron_dir);
    EXPECT_NE(std::string::npos, error_text.find(expected_warning));
}
