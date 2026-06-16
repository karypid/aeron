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

#ifndef AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H
#define AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H

#include "aeron_driver_context.h"
#include "aeron_name_resolver.h"
#include "concurrent/aeron_spsc_rb.h"

typedef struct aeron_driver_native_resource_agent_stct aeron_driver_native_resource_agent_t;

typedef enum aeron_driver_native_resource_agent_command_state_enum
{
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_PENDING,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_FAILED
}
aeron_driver_native_resource_agent_command_state_t;

typedef struct aeron_driver_native_resource_agent_command_result_stct
{
    volatile aeron_driver_native_resource_agent_command_state_t state;
    union
    {
        void *success;
        struct aeron_driver_native_resource_agent_command_error_stct
        {
            int code;
            char* message;
        }
        error;
    }
    payload;
}
aeron_driver_native_resource_agent_command_result_t;

typedef struct aeron_driver_native_resource_agent_proxy_stct
{
    aeron_driver_native_resource_agent_t *native_resource_agent;
    int64_t *fail_counter;
    aeron_spsc_rb_t *command_queue;
    aeron_spsc_rb_t *result_queue;
}
aeron_driver_native_resource_agent_proxy_t;

typedef int (*aeron_driver_native_resource_agent_task_on_execute_func_t)(void *task_clientd, void *conductor_clientd);
typedef void (*aeron_driver_native_resource_agent_task_on_cancel_func_t)(void *task_clientd);
typedef void (*aeron_driver_native_resource_agent_task_on_complete_func_t)(
    int execution_result, int errcode, const char *errmsg, void *task_clientd, void *conductor_clientd);

typedef struct aeron_driver_native_resource_agent_proxy_cmd_stct
{
    void (*execute)(aeron_driver_native_resource_agent_t *native_resource_agent, struct aeron_driver_native_resource_agent_proxy_cmd_stct *cmd);
    void (*cancel)(struct aeron_driver_native_resource_agent_proxy_cmd_stct *cmd);
}
aeron_driver_native_resource_agent_proxy_cmd_t;

typedef struct aeron_driver_native_resource_agent_proxy_cmd_resolve_address_stct
{
    aeron_driver_native_resource_agent_proxy_cmd_t base;
    aeron_name_resolver_async_resolve_t *address_resolution_params;
    aeron_driver_native_resource_agent_command_result_t *result;
}
aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t;

typedef struct aeron_driver_native_resource_agent_task_stct
{
    aeron_driver_native_resource_agent_proxy_cmd_t base;
    aeron_driver_native_resource_agent_task_on_execute_func_t on_execute;
    aeron_driver_native_resource_agent_task_on_complete_func_t on_complete;
    aeron_driver_native_resource_agent_task_on_cancel_func_t on_cancel;
    void *clientd;
    int result;
    int errcode;
    char errmsg[AERON_ERROR_MAX_TOTAL_LENGTH];
}
aeron_driver_native_resource_agent_task_t;

int aeron_driver_native_resource_agent_proxy_submit(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_on_execute_func_t on_execute,
    aeron_driver_native_resource_agent_task_on_complete_func_t on_complete,
    aeron_driver_native_resource_agent_task_on_cancel_func_t on_cancel,
    void *clientd);

void aeron_driver_native_resource_agent_proxy_on_task_complete(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_driver_native_resource_agent_task_t *task);


void aeron_driver_native_resource_agent_proxy_re_resolve_address(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_name_resolver_async_resolve_t *address_resolution_params,
    aeron_driver_native_resource_agent_command_result_t* result);

#endif //AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H
