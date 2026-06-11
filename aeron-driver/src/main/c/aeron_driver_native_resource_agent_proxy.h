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

typedef struct aeron_driver_native_resource_agent_stct aeron_driver_native_resource_agent_t;

typedef struct aeron_driver_native_resource_agent_proxy_stct
{
    aeron_driver_native_resource_agent_t *native_resource_agent;
    int64_t *fail_counter;
    aeron_mpsc_rb_t *command_queue;
    aeron_mpsc_rb_t *result_queue;
}
aeron_driver_native_resource_agent_proxy_t;

typedef int (*aeron_driver_native_resource_agent_task_on_execute_func_t)(void *task_clientd, void *conductor_clientd);
typedef void (*aeron_driver_native_resource_agent_task_on_cancel_func_t)(void *task_clientd);
typedef void (*aeron_driver_native_resource_agent_task_on_complete_func_t)(
    int execution_result, int errcode, const char *errmsg, void *task_clientd, void *conductor_clientd);

typedef struct aeron_driver_native_resource_agent_task_stct
{
    aeron_driver_native_resource_agent_t *native_resource_agent;
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

#endif //AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H
