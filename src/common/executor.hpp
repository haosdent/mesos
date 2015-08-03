/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/subprocess.hpp>

#include <stout/protobuf.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>

#include "messages/messages.hpp"

namespace mesos {
namespace internal {

using namespace process;

template <typename T>
class MesosExecutorProcess : public ProtobufProcess<T>
{
public:
  MesosExecutorProcess(const std::string& _healthCheckDir)
    : killedByHealthCheck(false),
      healthPid(-1),
      healthCheckDir(_healthCheckDir),
      driver(None()) {}

  virtual ~MesosExecutorProcess() {}

  virtual void registered(
      ExecutorDriver* _driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    std::cout << "Registered docker executor on " << slaveInfo.hostname()
              << std::endl;
    driver = _driver;
  }

  virtual void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    std::cout << "Re-registered docker executor on " << slaveInfo.hostname()
              << std::endl;
  }

  virtual void disconnected(ExecutorDriver* driver)
  {
    std::cout << "Disconnected from the slave" << std::endl;
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    shutdown(driver);
    if (healthPid != -1) {
      // Cleanup health check process.
      ::kill(healthPid, SIGKILL);
    }
  }

  virtual void shutdown(ExecutorDriver* driver) = 0;

protected:
  virtual void taskHealthUpdated(
      const TaskID& taskID,
      const bool& healthy,
      const bool& initiateTaskKill)
  {
    if (driver.isNone()) {
      return;
    }

    std::cout << "Received task health update, healthy: "
              << stringify(healthy) << std::endl;

    TaskStatus status;
    status.mutable_task_id()->CopyFrom(taskID);
    status.set_healthy(healthy);
    status.set_state(TASK_RUNNING);
    driver.get()->sendStatusUpdate(status);

    if (initiateTaskKill) {
      killedByHealthCheck = true;
      killTask(driver.get(), taskID);
    }
  }

  virtual void launchHealthCheck(const TaskInfo& task)
  {
    if (task.has_health_check()) {
      JSON::Object json = JSON::Protobuf(task.health_check());

      // Launch the subprocess using 'exec' style so that quotes can
      // be properly handled.
      std::vector<std::string> argv(4);
      argv[0] = "mesos-health-check";
      argv[1] = "--executor=" + stringify(this->self());
      argv[2] = "--health_check_json=" + stringify(json);
      argv[3] = "--task_id=" + task.task_id().value();

      std::cout << "Launching health check process: "
                << path::join(healthCheckDir, "mesos-health-check")
                << " " << argv[1] << " " << argv[2] << " " << argv[3]
                << std::endl;

      // Intentionally not sending STDIN to avoid health check
      // commands that expect STDIN input to block.
      Try<process::Subprocess> healthProcess = process::subprocess(
          path::join(healthCheckDir, "mesos-health-check"),
          argv,
          process::Subprocess::PATH("/dev/null"),
          process::Subprocess::FD(STDOUT_FILENO),
          process::Subprocess::FD(STDERR_FILENO));

      if (healthProcess.isError()) {
        std::cerr << "Unable to launch health process: "
                  << healthProcess.error();
      } else {
        healthPid = healthProcess.get().pid();

        std::cout << "Health check process launched at pid: "
                  << stringify(healthPid) << std::endl;
      }
    }
  }

  bool killedByHealthCheck;
  pid_t healthPid;
  std::string healthCheckDir;
  Option<ExecutorDriver*> driver;
};

} // namespace internal {
} // namespace mesos {
