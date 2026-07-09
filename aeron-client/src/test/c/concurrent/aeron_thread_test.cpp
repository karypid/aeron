/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
}

TEST(AeronThreadTest, shouldCreateReentrantMutex)
{
    aeron_mutex_t mutex;
    ASSERT_EQ(0, aeron_mutex_init(&mutex));

    EXPECT_EQ(0, aeron_mutex_lock(&mutex));
    EXPECT_EQ(0, aeron_mutex_lock(&mutex));
    EXPECT_EQ(0, aeron_mutex_unlock(&mutex));
    EXPECT_EQ(0, aeron_mutex_unlock(&mutex));

    EXPECT_EQ(0, aeron_mutex_destroy(&mutex));
}

TEST(AeronThreadTest, shouldCreateThreadWithAttributes)
{
    std::atomic<bool> called(false);
    auto testBlock = [](void *arg) -> void*
    {
        auto x = static_cast<std::atomic<bool>*>(arg);
        x->store(true);
        return nullptr;
    };

    aeron_thread_attr_t attr;
    EXPECT_EQ(0, aeron_thread_attr_init(&attr));

    aeron_thread_t thread;
    EXPECT_EQ(0, aeron_thread_create(&thread, &attr, testBlock, &called));

    EXPECT_EQ(0, aeron_thread_join(thread, nullptr));
    EXPECT_TRUE(called);
}

TEST(AeronThreadTest, shouldCreateThreadWithoutAttributes)
{
    std::atomic<bool> called(false);
    auto testBlock = [](void *arg) -> void*
    {
        auto x = static_cast<std::atomic<bool>*>(arg);
        x->store(true);
        return nullptr;
    };

    aeron_thread_t thread;
    EXPECT_EQ(0, aeron_thread_create(&thread, nullptr, testBlock, &called));

    EXPECT_EQ(0, aeron_thread_join(thread, nullptr));
    EXPECT_TRUE(called);
}

TEST(AeronThreadTest, getNameShouldFailIfBufferIsNull)
{
    EXPECT_EQ(-1, aeron_thread_get_name(nullptr, 1));
}

TEST(AeronThreadTest, getNameShouldFailIfBufferIsTooSmall)
{
    char buffer[128];
    EXPECT_EQ(-1, aeron_thread_get_name(buffer, 1));
    EXPECT_EQ(-1, aeron_thread_get_name(buffer, AERON_THREAD_NAME_MAX_LENGTH));
}

TEST(AeronThreadTest, shouldRejectAffinityLargerThanNumberOfAvailableCpus)
{
    aeron_err_clear();
#if defined(__linux__)
    EXPECT_EQ(-1, aeron_thread_set_affinity("test", UINT8_MAX)); // assumes that machine has less than 256 CPUs
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("failed to set thread affinity name=test, cpu_affinity_no=255"));
#else
    EXPECT_EQ(-1, aeron_thread_set_affinity("test", 0));
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("thread affinity not supported"));
#endif
    aeron_err_clear();
}

TEST(AeronThreadTest, shouldReturnCurrentAffinityAsASingleCpu)
{
    aeron_err_clear();
    uint8_t cpu_affinity_no = UINT8_MAX;
#if defined(__linux__)
    EXPECT_EQ(0, aeron_thread_get_affinity(&cpu_affinity_no));
    EXPECT_NE(UINT8_MAX, cpu_affinity_no);
#else
    EXPECT_EQ(-1, aeron_thread_get_affinity(&cpu_affinity_no));
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("thread affinity not supported"));
#endif
    aeron_err_clear();
}

TEST(AeronThreadTest, shouldChangeCpuAffinityOfARunningThread)
{
    aeron_err_clear();
#if defined(__linux__)
    std::atomic<bool> called(false);
    auto setAffinity = [](void *arg) -> void*
    {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        EXPECT_EQ(0, sched_getaffinity(0, sizeof(mask), &mask));

        for (uint8_t i = 0; i < UINT8_MAX; i++)
        {
            if (CPU_ISSET(i, &mask))
            {
                EXPECT_EQ(0, aeron_thread_set_affinity("test", i)) << aeron_errmsg();
                uint8_t cpu_affinity_no;
                EXPECT_EQ(0, aeron_thread_get_affinity(&cpu_affinity_no));
                EXPECT_EQ(i, cpu_affinity_no);
            }
        }

        auto x = static_cast<std::atomic<bool>*>(arg);
        x->store(true);
        return nullptr;
    };

    aeron_thread_attr_t attr;
    EXPECT_EQ(0, aeron_thread_attr_init(&attr));

    aeron_thread_t thread;
    EXPECT_EQ(0, aeron_thread_create(&thread, &attr, setAffinity, &called));

    EXPECT_EQ(0, aeron_thread_join(thread, nullptr));
    EXPECT_TRUE(called);
#endif
    aeron_err_clear();
}

class AeronThreadNameTest : public testing::TestWithParam<std::tuple<std::string, std::string>>
{
};

INSTANTIATE_TEST_SUITE_P(
    AeronThreadTest,
    AeronThreadNameTest,
    testing::Values(
      std::make_tuple("abc", "abc"),
      std::make_tuple("aeron-thread123", "aeron-thread123"),
      std::make_tuple("this thread name should be truncated as too long", "this thread nam"),
      std::make_tuple(std::string("1234567890abcde").append(1000, 'x'), "1234567890abcde")
    ));

TEST_P(AeronThreadNameTest, shouldSetThreadName)
{
    std::atomic<bool> called(false);
    auto setName = [](void *arg) -> void*
    {
        auto giveName = std::get<0>(GetParam());
        auto expectedName = std::get<1>(GetParam());

        EXPECT_EQ(0, aeron_thread_set_name(giveName.c_str())) << aeron_errmsg();

        char actualName[128];

        EXPECT_EQ(0, aeron_thread_get_name(actualName, sizeof(actualName)));
        EXPECT_STREQ(expectedName.c_str(), actualName);

        auto x = static_cast<std::atomic<bool>*>(arg);
        x->store(true);
        return nullptr;
    };

    aeron_thread_attr_t attr;
    EXPECT_EQ(0, aeron_thread_attr_init(&attr));

    aeron_thread_t thread;
    EXPECT_EQ(0, aeron_thread_create(&thread, &attr, setName, &called));

    EXPECT_EQ(0, aeron_thread_join(thread, nullptr));
    EXPECT_TRUE(called);
}
