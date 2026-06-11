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

#include "aeron_driver_native_resource_agent.h"
#include "aeron_driver_native_resource_agent_proxy.h"

static aeron_driver_native_resource_agent_task_t *aeron_driver_native_resource_agent_task_allocate(
    aeron_driver_native_resource_agent_t *native_resource_agent,
    aeron_driver_native_resource_agent_task_on_execute_func_t on_execute,
    aeron_driver_native_resource_agent_task_on_complete_func_t on_complete,
    aeron_driver_native_resource_agent_task_on_cancel_func_t on_cancel,
    void *clientd)
{
    aeron_driver_native_resource_agent_task_t *task;

    if (aeron_alloc((void **)&task, sizeof(aeron_driver_native_resource_agent_task_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

    task->native_resource_agent = native_resource_agent;
    task->on_execute = on_execute;
    task->on_complete = on_complete;
    task->on_cancel = on_cancel;
    task->clientd = clientd;
    task->result = -1;

    return task;
}

static void aeron_driver_native_resource_agent_proxy_offer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy, void *cmd, size_t length)
{
    aeron_rb_write_result_t result;
    while (AERON_RB_FULL == (result = aeron_mpsc_rb_write(native_resource_agent_proxy->command_queue, 1, cmd, length)))
    {
        aeron_counter_increment_release(native_resource_agent_proxy->fail_counter);
        sched_yield();
    }

    if (AERON_RB_ERROR == result)
    {
        aeron_distinct_error_log_record(
            native_resource_agent_proxy->native_resource_agent->context->error_log,
            EINVAL,
            "Error writing to native resource agent proxy ring buffer");
    }
}

int aeron_driver_native_resource_agent_proxy_submit(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_on_execute_func_t on_execute,
    aeron_driver_native_resource_agent_task_on_complete_func_t on_complete,
    aeron_driver_native_resource_agent_task_on_cancel_func_t on_cancel,
    void *clientd)
{
    aeron_driver_native_resource_agent_task_t *task = aeron_driver_native_resource_agent_task_allocate(
           native_resource_agent_proxy->native_resource_agent, on_execute, on_complete, on_cancel, clientd);
    if (NULL == task)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }
    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy, task, sizeof(aeron_driver_native_resource_agent_task_t));
    return 0;
}

void aeron_driver_native_resource_agent_proxy_on_task_complete(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_t *task)
{
    aeron_rb_write_result_t result;
    while (AERON_RB_FULL == (result = aeron_mpsc_rb_write(
        native_resource_agent_proxy->result_queue, 1, task, sizeof(aeron_driver_native_resource_agent_task_t))))
    {
        aeron_counter_increment_release(native_resource_agent_proxy->fail_counter);
        sched_yield();
    }

    if (AERON_RB_ERROR == result)
    {
        aeron_distinct_error_log_record(
            native_resource_agent_proxy->native_resource_agent->context->error_log,
            EINVAL,
            "Failed to write task result");
    }
}
