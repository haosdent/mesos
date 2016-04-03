// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __CONTAINERIZER_UTILS_HPP__
#define __CONTAINERIZER_UTILS_HPP__

#include <map>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include <process/pid.hpp>

#include <stout/try.hpp>

namespace mesos {
namespace internal {
namespace slave {

// Forward declaration.
class Flags;
class Slave;

// Determine slave resources from flags, probing the system or querying a
// delegate.
// TODO(idownes): Consider moving to containerizer implementations to enable a
// containerizer to best determine the resources, particularly if
// containerizeration is delegated.
Try<Resources> containerizerResources(const Flags& flags);


/**
 * Returns a map of environment variables necessary in order to launch
 * an executor.
 *
 * @param executorInfo ExecutorInfo being launched.
 * @param directory Path to the sandbox directory.
 * @param slaveId SlaveID where this executor is being launched.
 * @param slavePid PID of the slave launching the executor.
 * @param checkpoint Whether or not the framework is checkpointing.
 * @param flags Flags used to launch the slave.
 *
 * @return Map of environment variables (name, value).
 */
std::map<std::string, std::string> executorEnvironment(
    const ExecutorInfo& executorInfo,
    const std::string& directory,
    const SlaveID& slaveId,
    const process::UPID& slavePid,
    bool checkpoint,
    const Flags& flags,
    bool includeOsEnvironment = true);

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __CONTAINERIZER_UTILS_HPP__
