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
#include "aeron_driver_conductor.h"
#include "util/aeron_error.h"

static void aeron_driver_native_resource_agent_cancel_task(
    int32_t msg_type_id,
    const void *message,
    size_t size,
    void *clientd)
{
    aeron_driver_native_resource_agent_task_t *task = (aeron_driver_native_resource_agent_task_t *)message;
    task->on_cancel(task->clientd);
}

static void aeron_driver_native_resource_agent_execute_task(
    int32_t msg_type_id,
    const void *message,
    size_t size,
    void *clientd)
{
    aeron_driver_native_resource_agent_task_t *task = (aeron_driver_native_resource_agent_task_t *)message;
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;

    task->result = task->on_execute(task->clientd, native_resource_agent->conductor);
    if (task->result < 0)
    {
        task->errcode = aeron_errcode();
        memcpy(task->errmsg, aeron_errmsg(), strlen(aeron_errmsg()));
        aeron_err_clear();
    }

    aeron_driver_native_resource_agent_proxy_on_task_complete(&native_resource_agent->native_resource_agent_proxy, task);
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
        aeron_driver_native_resource_agent_execute_task,
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
    native_resource_agent->native_resource_agent_proxy.result_queue = &context->native_resource_agent_result_queue;
    native_resource_agent->native_resource_agent_proxy.fail_counter =
        aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);

    return 0;
}

void aeron_driver_native_resource_agent_on_close(void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;

    // free all scheduled but not executed commands
    while (0 != aeron_spsc_rb_read(
            native_resource_agent->native_resource_agent_proxy.command_queue,
            aeron_driver_native_resource_agent_cancel_task,
            native_resource_agent,
            SIZE_MAX))
    {
    }

    // free all completed but not consumed commands
    while (0 != aeron_spsc_rb_read(
            native_resource_agent->native_resource_agent_proxy.result_queue,
            aeron_driver_native_resource_agent_cancel_task,
            native_resource_agent,
            SIZE_MAX))
    {
    }

    native_resource_agent->name_resolver->close_func(native_resource_agent->name_resolver);
}
