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

#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/containerizer.hpp>

#include <mesos/slave/containerizer/containerizer.hpp>

#include <stout/try.hpp>

#include "slave/containerizer/hyper.hpp"

using namespace mesos;

using mesos::internal::slave::HyperContainerizer;

using mesos::slave::Containerizer;


// Declares a test Containerizer module named
// 'org_apache_mesos_TestHyperContainerizer'.
mesos::modules::Module<Containerizer>
org_apache_mesos_TestHyperContainerizer(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Hyper Containerizer module.",
    NULL,
    [](const Parameters& paramters) -> Containerizer* {
      Try<Containerizer*> result = HyperContainerizer::create(paramters);

      if (result.isError()) {
        return NULL;
      }

      return result.get();
    });
