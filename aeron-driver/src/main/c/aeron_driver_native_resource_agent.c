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
#include "aeron_alloc.h"
#include "command/aeron_control_protocol.h"
#include "util/aeron_error.h"

static void aeron_driver_native_resource_agent_signal_error(
    aeron_driver_native_resource_agent_t *native_resource_agent,
    aeron_driver_native_resource_agent_command_result_t *result)
{
    int errcode = aeron_errcode();
    const char* errmsg = aeron_errmsg();
    size_t errmsg_len = strlen(errmsg);

    if (aeron_alloc((void **)&result->payload.error.message, errmsg_len + 1) < 0)
    {
        // FIXME: It over-writes the original error message/code
        AERON_APPEND_ERR("failed to allocate error message: %s", aeron_errmsg());
        aeron_distinct_error_log_record(
            native_resource_agent->context->error_log, errcode, errmsg); // will be freed....
        aeron_err_clear();
    }
    else
    {
        memcpy(result->payload.error.message, errmsg, errmsg_len);
        result->payload.error.message[errmsg_len] = '\0';
    }
    result->payload.error.code = errcode;

    aeron_err_clear();

    AERON_SET_RELEASE(result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_FAILED);
}

static void aeron_driver_native_resource_agent_on_command(
    int32_t msg_type_id, const void *message, size_t size, void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;
    aeron_driver_native_resource_agent_proxy_cmd_t *cmd = (aeron_driver_native_resource_agent_proxy_cmd_t *)message;
    cmd->execute(native_resource_agent, cmd);
}

void aeron_driver_native_resource_agent_on_start(void *state, const char *role_name)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)state;

    if (NULL != native_resource_agent->name_resolver->start_func &&
        native_resource_agent->name_resolver->start_func(native_resource_agent->name_resolver) < 0)
    {
        if (0 != aeron_errcode())
        {
            AERON_APPEND_ERR("%s", "failed to start name resolver");
            aeron_distinct_error_log_record(
                native_resource_agent->context->error_log, aeron_errcode(), aeron_errmsg());
            aeron_err_clear();
        }
        else
        {
            aeron_distinct_error_log_record(
                native_resource_agent->context->error_log,
                -AERON_ERROR_CODE_GENERIC_ERROR,
                "failed to start name resolver");
        }
    }
}

int aeron_driver_native_resource_agent_do_work(void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;

    int work_count = 0;

    const int64_t now = native_resource_agent->context->epoch_clock();
    work_count += native_resource_agent->name_resolver->do_work_func(native_resource_agent->name_resolver, now);

    work_count += (int)aeron_spsc_rb_read(
        native_resource_agent->native_resource_agent_proxy.command_queue,
        aeron_driver_native_resource_agent_on_command,
        native_resource_agent,
    AERON_COMMAND_DRAIN_LIMIT);

    return work_count;
}

int aeron_driver_native_resource_agent_init(
    aeron_driver_native_resource_agent_t *native_resource_agent,
    aeron_name_resolver_t *name_resolver,
    aeron_driver_context_t *context,
    aeron_driver_conductor_t *conductor)
{
    native_resource_agent->name_resolver = name_resolver;
    native_resource_agent->context = context;
    native_resource_agent->conductor = conductor;

    // FIXME: create name resolve here...

    native_resource_agent->native_resource_agent_proxy.native_resource_agent = native_resource_agent;
    native_resource_agent->native_resource_agent_proxy.command_queue = &context->native_resource_agent_command_queue;
    native_resource_agent->native_resource_agent_proxy.fail_counter =
        aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);

    return 0;
}

void aeron_driver_native_resource_agent_on_close(void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;
    native_resource_agent->name_resolver->close_func(native_resource_agent->name_resolver);
}

void aeron_driver_native_resource_agent_on_resolve_address(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd)
{
    aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t *resolve_cmd =
        (aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t *)cmd;
    if (aeron_name_resolver_resolve_host_and_port(
        native_resource_agent->name_resolver,
        resolve_cmd->address_resolution_params->endpoint_name,
        resolve_cmd->address_resolution_params->uri_param_name,
        resolve_cmd->address_resolution_params->is_re_resolution,
        &resolve_cmd->address_resolution_params->resolved_address) < 0)
    {
        AERON_APPEND_ERR("%s", "address re-resolution failed");
        aeron_driver_native_resource_agent_signal_error(native_resource_agent, resolve_cmd->result);
    }
    else
    {
        // FIXME: Store resolved address in the success state instead of `aeron_name_resolver_async_resolve_t->resolved_address`
        AERON_SET_RELEASE(resolve_cmd->result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED);
    }
}

void aeron_driver_native_resource_agent_on_parse_udp_channel(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd)
{
    aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t *channel_cmd =
        (aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t *)cmd;
    if (aeron_udp_channel_finish_parse(native_resource_agent->name_resolver, channel_cmd->async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to parse channel");
        aeron_driver_native_resource_agent_signal_error(native_resource_agent, channel_cmd->result);
    }
    else
    {
        AERON_SET_RELEASE(channel_cmd->result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED);
    }
}
