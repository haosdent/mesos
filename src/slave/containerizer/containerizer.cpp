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
#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/docker.hpp"
#include "slave/containerizer/external_containerizer.hpp"

#include "slave/containerizer/mesos/containerizer.hpp"
#include "slave/containerizer/mesos/launcher.hpp"
#ifdef __linux__
#include "slave/containerizer/mesos/linux_launcher.hpp"
#endif // __linux__

using flags::FlagsBase;

using std::map;
using std::string;
using std::vector;

using namespace process;

namespace mesos {
namespace internal {
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


map<string, string> executorEnvironment(
    const ExecutorInfo& executorInfo,
    const string& directory,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint,
    const Flags& flags,
    bool includeOsEnvironment)
{
  map<string, string> environment;

  // In cases where DNS is not available on the slave, the absence of
  // LIBPROCESS_IP in the executor's environment will cause an error when the
  // new executor process attempts a hostname lookup. Thus, we pass the slave's
  // LIBPROCESS_IP through here, even if the executor environment is specified
  // explicitly. Note that a LIBPROCESS_IP present in the provided flags will
  // override this value.
  Option<string> libprocessIP = os::getenv("LIBPROCESS_IP");
  if (libprocessIP.isSome()) {
    environment["LIBPROCESS_IP"] = libprocessIP.get();
  }

  if (flags.executor_environment_variables.isSome()) {
    foreachpair (const string& key,
                 const JSON::Value& value,
                 flags.executor_environment_variables.get().values) {
      // See slave/flags.cpp where we validate each value is a string.
      CHECK(value.is<JSON::String>());
      environment[key] = value.as<JSON::String>().value;
    }
  } else if (includeOsEnvironment) {
    environment = os::environment();
  }

  // Set LIBPROCESS_PORT so that we bind to a random free port (since
  // this might have been set via --port option). We do this before
  // the environment variables below in case it is included.
  environment["LIBPROCESS_PORT"] = "0";

  // Also add MESOS_NATIVE_JAVA_LIBRARY if it's not already present (and
  // like above, we do this before the environment variables below in
  // case the framework wants to override).
  // TODO(tillt): Adapt library towards JNI specific name once libmesos
  // has been split.
  if (environment.count("MESOS_NATIVE_JAVA_LIBRARY") == 0) {
    string path =
#ifdef __APPLE__
      LIBDIR "/libmesos-" VERSION ".dylib";
#else
      LIBDIR "/libmesos-" VERSION ".so";
#endif
    if (os::exists(path)) {
      environment["MESOS_NATIVE_JAVA_LIBRARY"] = path;
    }
  }

  // Also add MESOS_NATIVE_LIBRARY if it's not already present.
  // This environment variable is kept for offering non JVM-based
  // frameworks a more compact and JNI independent library.
  if (environment.count("MESOS_NATIVE_LIBRARY") == 0) {
    string path =
#ifdef __APPLE__
      LIBDIR "/libmesos-" VERSION ".dylib";
#else
      LIBDIR "/libmesos-" VERSION ".so";
#endif
    if (os::exists(path)) {
      environment["MESOS_NATIVE_LIBRARY"] = path;
    }
  }

  environment["MESOS_FRAMEWORK_ID"] = executorInfo.framework_id().value();
  environment["MESOS_EXECUTOR_ID"] = executorInfo.executor_id().value();
  environment["MESOS_DIRECTORY"] = directory;
  environment["MESOS_SLAVE_ID"] = slaveId.value();
  environment["MESOS_SLAVE_PID"] = stringify(slavePid);
  environment["MESOS_AGENT_ENDPOINT"] = stringify(slavePid.address);
  environment["MESOS_CHECKPOINT"] = checkpoint ? "1" : "0";

  // Set executor's shutdown grace period. If set, the customized value
  // from `ExecutorInfo` overrides the default from agent flags.
  Duration executorShutdownGracePeriod = flags.executor_shutdown_grace_period;
  if (executorInfo.has_shutdown_grace_period()) {
    executorShutdownGracePeriod =
      Nanoseconds(executorInfo.shutdown_grace_period().nanoseconds());
  }

  environment["MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD"] =
    stringify(executorShutdownGracePeriod);

  if (checkpoint) {
    environment["MESOS_RECOVERY_TIMEOUT"] = stringify(flags.recovery_timeout);

    // The maximum backoff duration to be used by an executor between two
    // retries when disconnected.
    environment["MESOS_SUBSCRIPTION_BACKOFF_MAX"] =
      stringify(EXECUTOR_REREGISTER_TIMEOUT);
  }

  if (HookManager::hooksAvailable()) {
    // Include any environment variables from Hooks.
    // TODO(karya): Call environment decorator hook _after_ putting all
    // variables from executorInfo into 'env'. This would prevent the
    // ones provided by hooks from being overwritten by the ones in
    // executorInfo in case of a conflict. The overwriting takes places
    // at the callsites of executorEnvironment (e.g., ___launch function
    // in src/slave/containerizer/docker.cpp)
    // TODO(karya): Provide a mechanism to pass the new environment
    // variables created above (MESOS_*) on to the hook modules.
    const Environment& hooksEnvironment =
      HookManager::slaveExecutorEnvironmentDecorator(executorInfo);

    foreach (const Environment::Variable& variable,
             hooksEnvironment.variables()) {
      environment[variable.name()] = variable.value();
    }
  }

  return environment;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
