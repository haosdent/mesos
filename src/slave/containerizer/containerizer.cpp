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

#include <map>
#include <vector>

#include <mesos/slave/containerizer/containerizer.hpp>

#include <process/dispatch.hpp>
#include <process/owned.hpp>

#include <stout/fs.hpp>
#include <stout/hashmap.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/uuid.hpp>

#include "hook/manager.hpp"

#include "slave/flags.hpp"
#include "slave/slave.hpp"

#include "slave/containerizer/composing.hpp"
#include "slave/containerizer/docker.hpp"
#include "slave/containerizer/external_containerizer.hpp"

#include "slave/containerizer/mesos/containerizer.hpp"
#include "slave/containerizer/mesos/launcher.hpp"
#ifdef __linux__
#include "slave/containerizer/mesos/linux_launcher.hpp"
#endif // __linux__

using flags::FlagsBase;

using mesos::internal::slave::Flags;
using mesos::internal::slave::ComposingContainerizer;
using mesos::internal::slave::DockerContainerizer;
using mesos::internal::slave::ExternalContainerizer;
using mesos::internal::slave::MesosContainerizer;

using std::map;
using std::string;
using std::vector;

using namespace process;

namespace mesos {
namespace slave {

Parameters Containerizer::parameterize(
    const FlagsBase& flags,
    const Option<bool>& local)
{
  Parameters parameters;

  Parameter* parameter = parameters.add_parameter();
  parameter->set_key("flags");
  parameter->set_value(string(jsonify(flags)));

  if (local.isSome()) {
    parameter = parameters.add_parameter();
    parameter->set_key("local");
    parameter->set_value(stringify(local.get()));
  }

  return parameters;
}


Try<Containerizer*> Containerizer::create(const Parameters& parameters)
{
  Flags flags;

  foreach (const Parameter& parameter, parameters.parameter()) {
    if (parameter.key() == "flags") {
      Try<map<string, string>> values =
        flags::parse<map<string, string>>(parameter.value());

      if (values.isError()) {
        LOG(ERROR) << "Failed to parse 'flags' parameter in '"
                   << jsonify(JSON::Protobuf(parameters)) << "': "
                   << values.error();
        return Error("Failed to parse 'flags' parameter:" + values.error());
      }

      // Unkown flag is forbidden.
      Try<Nothing> load = flags.load(values.get(), false);
      if (load.isError()) {
        LOG(ERROR) << "Failed to load flags '" << stringify(values.get())
                   << "': " << load.error();
        return Error("Failed to load flags: " + load.error());
      }
    }
  }

  if (flags.isolation == "external") {
    LOG(WARNING) << "The 'external' isolation flag is deprecated, "
                 << "please update your flags to"
                 << " '--containerizers=external'.";

    if (flags.container_logger.isSome()) {
      return Error(
          "The external containerizer does not support custom container "
          "logger modules.  The '--isolation=external' flag cannot be "
          " set along with '--container_logger=...'");
    }

    Try<Containerizer*> containerizer =
      ExternalContainerizer::create(parameters);
    if (containerizer.isError()) {
      return Error("Could not create ExternalContainerizer: " +
                   containerizer.error());
    }

    return containerizer.get();
  }

  // TODO(benh): We need to store which containerizer or
  // containerizers were being used. See MESOS-1663.

  // Create containerizer(s).
  vector<Containerizer*> containerizers;

  foreach (const string& type, strings::split(flags.containerizers, ",")) {
    if (type == "mesos") {
      Try<Containerizer*> containerizer =
        MesosContainerizer::create(parameters);
      if (containerizer.isError()) {
        return Error("Could not create MesosContainerizer: " +
                     containerizer.error());
      } else {
        containerizers.push_back(containerizer.get());
      }
    } else if (type == "docker") {
      Try<Containerizer*> containerizer =
        DockerContainerizer::create(parameters);
      if (containerizer.isError()) {
        return Error("Could not create DockerContainerizer: " +
                     containerizer.error());
      } else {
        containerizers.push_back(containerizer.get());
      }
    } else if (type == "external") {
      if (flags.container_logger.isSome()) {
        return Error(
            "The external containerizer does not support custom container "
            "logger modules.  The '--containerizers=external' flag cannot be "
            "set along with '--container_logger=...'");
      }

      Try<Containerizer*> containerizer =
        ExternalContainerizer::create(parameters);
      if (containerizer.isError()) {
        return Error("Could not create ExternalContainerizer: " +
                     containerizer.error());
      } else {
        containerizers.push_back(containerizer.get());
      }
    } else {
      return Error("Unknown or unsupported containerizer: " + type);
    }
  }

  if (containerizers.size() == 1) {
    return containerizers.front();
  }

  Try<Containerizer*> containerizer =
    ComposingContainerizer::create(containerizers);

  if (containerizer.isError()) {
    return Error(containerizer.error());
  }

  return containerizer.get();
}

} // namespace slave {
} // namespace mesos {
