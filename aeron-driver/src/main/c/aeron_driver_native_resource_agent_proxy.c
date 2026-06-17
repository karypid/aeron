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

static void aeron_driver_native_resource_agent_proxy_offer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_proxy_cmd_t *cmd,
    size_t length)
{
    while (AERON_RB_SUCCESS != aeron_spsc_rb_write(native_resource_agent_proxy->command_queue, 1, cmd, length))
    {
        aeron_counter_increment_release(native_resource_agent_proxy->fail_counter);
        sched_yield();
    }
}

int aeron_driver_native_resource_agent_proxy_submit(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_on_execute_func_t on_execute,
    aeron_driver_native_resource_agent_task_on_complete_func_t on_complete,
    aeron_driver_native_resource_agent_task_on_cancel_func_t on_cancel,
    void *clientd)
{
    aeron_driver_native_resource_agent_task_t task;
    task.base.execute = aeron_driver_native_resource_agent_on_task_execute;
    task.base.cancel = aeron_driver_native_resource_agent_on_task_cancel;
    task.on_execute = on_execute;
    task.on_complete = on_complete;
    task.on_cancel = on_cancel;
    task.clientd = clientd;
    task.result = -1;

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&task,
        sizeof(aeron_driver_native_resource_agent_task_t));
    return 0;
}

void aeron_driver_native_resource_agent_proxy_on_task_complete(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_t *task)
{
    while (AERON_RB_SUCCESS != aeron_spsc_rb_write(
        native_resource_agent_proxy->result_queue, 1, task, sizeof(aeron_driver_native_resource_agent_task_t)))
    {
        aeron_counter_increment_release(native_resource_agent_proxy->fail_counter);
        sched_yield();
    }
}

static void aeron_driver_native_resource_agent_proxy_init_result(
    aeron_driver_native_resource_agent_command_result_t* result)
{
    result->payload.success = NULL;
    result->payload.error.code = 0;
    result->payload.error.message = NULL;
    result->state = AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_PENDING;
}

void aeron_driver_native_resource_agent_proxy_re_resolve_address(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_name_resolver_async_resolve_t *address_resolution_params,
    aeron_driver_native_resource_agent_command_result_t* result)
{
    aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t cmd;
    cmd.base.execute = aeron_driver_native_resource_agent_on_re_resolve_address;
    cmd.base.cancel = NULL;
    cmd.address_resolution_params = address_resolution_params;
    cmd.result = result;
    aeron_driver_native_resource_agent_proxy_init_result(result);

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&cmd,
        sizeof(aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t));
}

void aeron_driver_native_resource_agent_proxy_parse_udp_channel(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_udp_channel_async_parse_t *async_parse,
    aeron_driver_native_resource_agent_command_result_t *result)
{
    aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t cmd;
    cmd.base.execute = aeron_driver_native_resource_agent_on_parse_udp_channel;
    cmd.base.cancel = NULL;
    cmd.async_parse = async_parse;
    cmd.result = result;
    aeron_driver_native_resource_agent_proxy_init_result(result);

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&cmd,
        sizeof(aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t));
}
