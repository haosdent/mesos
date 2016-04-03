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

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __CONTAINERIZER_UTILS_HPP__
