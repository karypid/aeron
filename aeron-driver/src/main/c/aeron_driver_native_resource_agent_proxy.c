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


void aeron_driver_native_resource_agent_proxy_resolve_address(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_name_resolver_async_resolve_t *address_resolution_params,
    aeron_driver_native_resource_agent_command_result_t* result)
{
    aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t cmd;
    cmd.base.execute = aeron_driver_native_resource_agent_on_resolve_address;
    cmd.address_resolution_params = address_resolution_params;
    cmd.result = result;

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
    cmd.async_parse = async_parse;
    cmd.result = result;

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&cmd,
        sizeof(aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t));
}

void aeron_driver_native_resource_agent_proxy_free_log_buffer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *log_file_name)
{
    aeron_driver_native_resource_agent_proxy_cmd_free_log_buffer_t cmd;
    cmd.base.execute = aeron_driver_native_resource_agent_on_free_log_buffer;
    cmd.mapped_raw_log = mapped_raw_log;
    cmd.log_file_name = log_file_name;

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&cmd,
    sizeof(aeron_driver_native_resource_agent_proxy_cmd_free_log_buffer_t));
}

void aeron_driver_native_resource_agent_proxy_map_log_buffer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    const char *log_file_name,
    size_t term_length,
    bool is_sparse,
    aeron_driver_native_resource_agent_command_result_t *result)
{
    aeron_driver_native_resource_agent_proxy_cmd_map_log_buffer_t cmd;
    cmd.base.execute = aeron_driver_native_resource_agent_on_map_log_buffer;
    cmd.log_file_name = log_file_name;
    cmd.term_length = term_length;
    cmd.is_sparse = is_sparse;
    cmd.result = result;

    aeron_driver_native_resource_agent_proxy_offer(
        native_resource_agent_proxy,
        (aeron_driver_native_resource_agent_proxy_cmd_t *)&cmd,
    sizeof(aeron_driver_native_resource_agent_proxy_cmd_map_log_buffer_t));
}
