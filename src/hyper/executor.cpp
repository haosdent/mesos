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

#include <stdio.h>

#include <iostream>
#include <map>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/executor.hpp>

#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/subprocess.hpp>
#include <process/reap.hpp>
#include <process/owned.hpp>

#include <stout/flags.hpp>
#include <stout/protobuf.hpp>
#include <stout/os.hpp>

#include "common/command_utils.hpp"
#include "common/status_utils.hpp"

#include "hyper/executor.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "mesos/resources.hpp"

#include "messages/messages.hpp"

#include "slave/constants.hpp"

using namespace mesos;
using namespace process;

using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace hyper {

const Duration HYPER_INSPECT_DELAY = Milliseconds(500);
const Duration HYPER_INSPECT_TIMEOUT = Seconds(5);

const string HYPER_PATH = "hyper";

// Executor that is responsible to execute a hyper container and
// redirect log output to configured stdout and stderr files. Similar
// to `CommandExecutor`, it launches a single task (a hyper container)
// and exits after the task finishes or is killed. The executor assumes
// that it is launched from the `HyperContainerizer`.
class HyperExecutorProcess : public ProtobufProcess<HyperExecutorProcess>
{
public:
  HyperExecutorProcess(
      const string& containerName,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const Duration& shutdownGracePeriod)
    : killed(false),
      containerName(containerName),
      sandboxDirectory(sandboxDirectory),
      mappedDirectory(mappedDirectory),
      shutdownGracePeriod(shutdownGracePeriod) {}

  virtual ~HyperExecutorProcess() {}

  void registered(
      ExecutorDriver* _driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& _frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    cout << "Registered hyper executor on " << slaveInfo.hostname() << endl;
    driver = _driver;
    frameworkInfo = _frameworkInfo;
  }

  void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered hyper executor on " << slaveInfo.hostname() << endl;
  }

  void disconnected(ExecutorDriver* driver)
  {
    cout << "Disconnected from the slave" << endl;
  }

  void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    if (run.isSome()) {
      TaskStatus status;
      status.mutable_task_id()->CopyFrom(task.task_id());
      status.set_state(TASK_FAILED);
      status.set_message(
          "Attempted to run multiple tasks using a \"hyper\" executor");

      driver->sendStatusUpdate(status);
      return;
    }

    // Capture the TaskID.
    taskId = task.task_id();

    // Capture the kill policy.
    if (task.has_kill_policy()) {
      killPolicy = task.kill_policy();
    }

    cout << "Starting task " << taskId.get() << endl;

    CHECK(task.has_container());
    CHECK(task.has_command());

    // We're adding task and executor resources to launch hyper since
    // the HyperContainerizer updates the container cgroup limits
    // directly and it expects it to be the sum of both task and
    // executor resources. This does leave to a bit of unaccounted
    // resources for running this executor, but we are assuming
    // this is just a very small amount of overcommit.
    run = _run(
        task.container(),
        task.command(),
        containerName,
        sandboxDirectory,
        mappedDirectory,
        task.resources() + task.executor().resources(),
        None(),
        Subprocess::FD(STDOUT_FILENO),
        Subprocess::FD(STDERR_FILENO));

    run->onAny(defer(self(), &Self::reaped, driver, lambda::_1));

    // Delay sending TASK_RUNNING status update until we receive
    // inspect output.
    TaskStatus status;
    status.mutable_task_id()->CopyFrom(taskId.get());
    status.set_state(TASK_RUNNING);
    driver->sendStatusUpdate(status);
  }

  Future<Nothing> _run(
      const ContainerInfo& containerInfo,
      const CommandInfo& commandInfo,
      const string& name,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const Option<Resources>& resources,
      const Option<map<string, string>>& env,
      const process::Subprocess::IO& stdout,
      const process::Subprocess::IO& stderr) const
  {
    Option<string> image;
    foreach (const Parameter& parameter, containerInfo.custom().parameters()) {
      if (parameter.key() == "image") {
        image = parameter.value();
      }
    }

    vector<string> argv = {
      HYPER_PATH,
      "run",
      image.get(),
      "--name=" + name,
    };

    if (commandInfo.shell()) {
      if (!commandInfo.has_value()) {
        return Failure("Shell specified but no command value provided");
      }

      // Adding -c here because Docker cli only supports a single word
      // for overriding entrypoint, so adding the -c flag for /bin/sh
      // as part of the command.
      argv.push_back("sh");
      argv.push_back("-c");
      argv.push_back(commandInfo.value());
    } else {
      if (commandInfo.has_value()) {
        argv.push_back(commandInfo.value());
      }

      foreach (const string& argument, commandInfo.arguments()) {
        argv.push_back(argument);
      }
    }

    map<string, string> environment = os::environment();

    string cmd = strings::join(" ", argv);

    LOG(INFO) << "Launch hyper container: " << cmd;

    Try<Subprocess> s = subprocess(
        HYPER_PATH,
        argv,
        Subprocess::PATH("/dev/null"),
        stdout,
        stderr,
        NO_SETSID,
        None(),
        environment);

    if (s.isError()) {
      return Failure(s.error());
    }

    // We don't call checkError here to avoid printing the stderr
    // of the docker container task as docker run with attach forwards
    // the container's stderr to the client's stderr.
    return s.get().status()
      .then(defer(self(), &Self::__run, lambda::_1));
  }

  Future<Nothing> __run(const Option<int>& status)
  {
    if (status.isNone()) {
      return Failure("Failed to get exit status");
    } else if (status.get() != 0) {
      return Failure("Container exited on error: " + WSTRINGIFY(status.get()));
    }

    return Nothing();
  }

  void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    cout << "Received killTask for task " << taskId.value() << endl;

    // Using shutdown grace period as a default is backwards compatible
    // with the `stop_timeout` flag, deprecated in 0.29.
    Duration gracePeriod = shutdownGracePeriod;

    if (killPolicy.isSome() && killPolicy->has_grace_period()) {
      gracePeriod = Nanoseconds(killPolicy->grace_period().nanoseconds());
    }

    killTask(driver, taskId, gracePeriod);
  }

  void frameworkMessage(ExecutorDriver* driver, const string& data) {}

  void shutdown(ExecutorDriver* driver)
  {
    cout << "Shutting down" << endl;

    // Currently, 'hyper->run' uses the reaper internally, hence we need
    // to account for the reap interval. We also leave a small buffer of
    // time to do the forced kill, otherwise the agent may destroy the
    // container before we can send `TASK_KILLED`.
    //
    // TODO(alexr): Remove `MAX_REAP_INTERVAL` once the reaper signals
    // immediately after the watched process has exited.
    Duration gracePeriod =
      shutdownGracePeriod - process::MAX_REAP_INTERVAL() - Seconds(1);

    // Since the hyper executor manages a single task,
    // shutdown boils down to killing this task.
    //
    // TODO(bmahler): If a shutdown arrives after a kill task within
    // the grace period of the `KillPolicy`, we may need to escalate
    // more quickly (e.g. the shutdown grace period allotted by the
    // agent is smaller than the kill grace period).
    if (run.isSome()) {
      CHECK_SOME(taskId);
      killTask(driver, taskId.get(), gracePeriod);
    } else {
      driver->stop();
    }
  }

  void error(ExecutorDriver* driver, const string& message) {}

protected:
  virtual void initialize()
  {
  }

private:
  void killTask(
      ExecutorDriver* driver,
      const TaskID& _taskId,
      const Duration& gracePeriod)
  {
    if (run.isSome() && !killed) {
      // Send TASK_KILLING if the framework can handle it.
      CHECK_SOME(frameworkInfo);
      CHECK_SOME(taskId);
      CHECK(taskId.get() == _taskId);

      foreach (const FrameworkInfo::Capability& c,
               frameworkInfo->capabilities()) {
        if (c.type() == FrameworkInfo::Capability::TASK_KILLING_STATE) {
          TaskStatus status;
          status.mutable_task_id()->CopyFrom(taskId.get());
          status.set_state(TASK_KILLING);
          driver->sendStatusUpdate(status);
          break;
        }
      }

      // The hyper daemon might still be in progress starting the
      // container, therefore we kill both the hyper run process
      // and also ask the daemon to stop the container.
      run->discard();
      stop(containerName);
      killed = true;
    }
  }

  Future<Nothing> stop(const string& containerId)
  {
    LOG(INFO) << "Stop hyper container: " << containerId;

    Try<string> shell = os::shell(
        "hyper list|grep %s|awk {'print $1'}|xargs hyper rm",
        containerId.c_str());
    if (shell.isError()) {
      return Failure(shell.error());
    } else {
      return Nothing();
    }
  }

  void reaped(
      ExecutorDriver* _driver,
      const Future<Nothing>& run)
  {
    // stop(containerName);

    TaskStatus taskStatus;
    taskStatus.mutable_task_id()->CopyFrom(taskId.get());
    taskStatus.set_state(TASK_FINISHED);

    driver.get()->sendStatusUpdate(taskStatus);

    // A hack for now ... but we need to wait until the status update
    // is sent to the slave before we shut ourselves down.
    // TODO(tnachen): Remove this hack and also the same hack in the
    // command executor when we have the new HTTP APIs to wait until
    // an ack.
    os::sleep(Seconds(1));
    driver.get()->stop();
  }

  bool killed;
  string containerName;
  string sandboxDirectory;
  string mappedDirectory;
  Duration shutdownGracePeriod;
  Option<KillPolicy> killPolicy;
  Option<Future<Nothing>> run;
  Option<ExecutorDriver*> driver;
  Option<FrameworkInfo> frameworkInfo;
  Option<TaskID> taskId;
};


class HyperExecutor : public Executor
{
public:
  HyperExecutor(
      const string& container,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const Duration& shutdownGracePeriod)
  {
    process = Owned<HyperExecutorProcess>(new HyperExecutorProcess(
        container,
        sandboxDirectory,
        mappedDirectory,
        shutdownGracePeriod));

    spawn(process.get());
  }

  virtual ~HyperExecutor()
  {
    terminate(process.get());
    wait(process.get());
  }

  virtual void registered(
      ExecutorDriver* driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process.get(),
             &HyperExecutorProcess::registered,
             driver,
             executorInfo,
             frameworkInfo,
             slaveInfo);
  }

  virtual void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process.get(),
             &HyperExecutorProcess::reregistered,
             driver,
             slaveInfo);
  }

  virtual void disconnected(ExecutorDriver* driver)
  {
    dispatch(process.get(), &HyperExecutorProcess::disconnected, driver);
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    dispatch(process.get(), &HyperExecutorProcess::launchTask, driver, task);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    dispatch(process.get(), &HyperExecutorProcess::killTask, driver, taskId);
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data)
  {
    dispatch(process.get(),
             &HyperExecutorProcess::frameworkMessage,
             driver,
             data);
  }

  virtual void shutdown(ExecutorDriver* driver)
  {
    dispatch(process.get(), &HyperExecutorProcess::shutdown, driver);
  }

  virtual void error(ExecutorDriver* driver, const string& data)
  {
    dispatch(process.get(), &HyperExecutorProcess::error, driver, data);
  }

private:
  Owned<HyperExecutorProcess> process;
};


} // namespace hyper {
} // namespace internal {
} // namespace mesos {


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  mesos::internal::hyper::Flags flags;

  // Load flags from environment and command line.
  Try<Nothing> load = flags.load(None(), &argc, &argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  std::cout << stringify(flags) << std::endl;

  mesos::internal::logging::initialize(argv[0], flags, true); // Catch signals.

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  std::cout << stringify(flags) << std::endl;

  if (flags.container.isNone()) {
    cerr << flags.usage("Missing required option --container") << endl;
    return EXIT_FAILURE;
  }

  if (flags.sandbox_directory.isNone()) {
    cerr << flags.usage("Missing required option --sandbox_directory") << endl;
    return EXIT_FAILURE;
  }

  if (flags.mapped_directory.isNone()) {
    cerr << flags.usage("Missing required option --mapped_directory") << endl;
    return EXIT_FAILURE;
  }

  // Get executor shutdown grace period from the environment.
  //
  // NOTE: We avoided introducing a hyper executor flag for this
  // because the hyper executor exits if it sees an unknown flag.
  // This makes it difficult to add or remove hyper executor flags
  // that are unconditionally set by the agent.
  Duration shutdownGracePeriod =
    mesos::internal::slave::DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD;
  Option<string> value = os::getenv("MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD");
  if (value.isSome()) {
    Try<Duration> parse = Duration::parse(value.get());
    if (parse.isError()) {
      cerr << "Failed to parse value '" << value.get() << "'"
           << " of 'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': " << parse.error();
      return EXIT_FAILURE;
    }

    shutdownGracePeriod = parse.get();
  }

  // If the deprecated flag is set, respect it and choose the bigger value.
  //
  // TODO(alexr): Remove this after the deprecation cycle (started in 0.29).
  if (flags.stop_timeout.isSome() &&
      flags.stop_timeout.get() > shutdownGracePeriod) {
    shutdownGracePeriod = flags.stop_timeout.get();
  }

  if (flags.launcher_dir.isNone()) {
    cerr << flags.usage("Missing required option --launcher_dir") << endl;
    return EXIT_FAILURE;
  }

  mesos::internal::hyper::HyperExecutor executor(
      flags.container.get(),
      flags.sandbox_directory.get(),
      flags.mapped_directory.get(),
      shutdownGracePeriod);

  mesos::MesosExecutorDriver driver(&executor);
  return driver.run() == mesos::DRIVER_STOPPED ? EXIT_SUCCESS : EXIT_FAILURE;
}
