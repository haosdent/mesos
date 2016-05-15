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

#include <list>
#include <map>
#include <string>
#include <vector>

#include <mesos/slave/container_logger.hpp>

#include <mesos/slave/containerizer/containerizer.hpp>

#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/io.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>

#include <stout/adaptor.hpp>
#include <stout/fs.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>

#include "common/command_utils.hpp"
#include "common/status_utils.hpp"

#include "slave/paths.hpp"
#include "slave/slave.hpp"

#include "slave/containerizer/fetcher.hpp"
#include "slave/containerizer/hyper.hpp"


using std::list;
using std::map;
using std::string;
using std::vector;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

using mesos::slave::ContainerLogger;
using mesos::slave::Containerizer;
using mesos::slave::ContainerState;

const string CONTAINER_HYPER = "hyper";

// Declared in header, see explanation there.
const string HYPER_NAME_PREFIX = "mesos-";

// Declared in header, see explanation there.
const string HYPER_NAME_SEPERATOR = ".";

// Declared in header, see explanation there.
const string HYPER_SYMLINK_DIRECTORY = "hyper/links";

const string HYPER_PATH = "hyper";

constexpr Duration HYPER_STOP_TIMEOUT = Seconds(0);

constexpr Duration HYPER_REMOVE_DELAY = Hours(6);

// Parse the ContainerID from a Hyper container and return None if
// the container was not launched from Mesos.
Option<ContainerID> parse(const string& containerName)
{
  // FIXME(haosdent)
  if (!strings::startsWith(containerName, HYPER_NAME_PREFIX)) {
    return None();
  }

  string name = strings::remove(
      containerName, HYPER_NAME_PREFIX, strings::PREFIX);
  vector<string> parts = strings::split(name, HYPER_NAME_SEPERATOR);
  if (parts.size() == 3) {
    ContainerID id;
    id.set_value(parts[1]);
    return id;
  }

  return None();
}


Try<Containerizer*> HyperContainerizer::create(const Parameters& parameters)
{
  Flags flags;
  bool local;

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
      Try<Nothing> load = flags.load(values.get(), true);
      if (load.isError()) {
        LOG(ERROR) << "Failed to load flags '" << stringify(values.get())
                   << "': " << load.error();
        return Error("Failed to load flags: " + load.error());
      }
    } else if (parameter.key() == "local") {
      Try<bool> local_  = numify<bool>(parameter.value());

      if (local_.isError()) {
        LOG(ERROR) << "Failed to parse 'local' parameter in '"
                   << jsonify(JSON::Protobuf(parameters)) << "': "
                   << local_.error();
        return Error("Failed to parse 'local' parameter:" + local_.error());
      }

      local = local_.get();
    }
  }

  // Create and initialize the container logger module.
  Try<ContainerLogger*> logger =
    ContainerLogger::create(flags.container_logger);

  if (logger.isError()) {
    return Error("Failed to create container logger: " + logger.error());
  }

  return new HyperContainerizer(
      flags,
      local,
      Owned<Fetcher>(new Fetcher()),
      Owned<ContainerLogger>(logger.get()));
}


HyperContainerizer::HyperContainerizer(
    const Owned<HyperContainerizerProcess>& _process)
  : process(_process)
{
  spawn(process.get());
}


HyperContainerizer::HyperContainerizer(
    const Flags& flags,
    bool local,
    const Owned<Fetcher>& fetcher,
    const Owned<ContainerLogger>& logger)
  : process(new HyperContainerizerProcess(
      flags,
      local,
      fetcher,
      logger))
{
  spawn(process.get());
}


HyperContainerizer::~HyperContainerizer()
{
  terminate(process.get());
  process::wait(process.get());
}


hyper::Flags hyperFlags(
  const Flags& flags,
  const string& name,
  const string& directory)
{
  hyper::Flags hyperFlags;
  hyperFlags.container = name;
  hyperFlags.docker = flags.docker;
  hyperFlags.sandbox_directory = directory;
  hyperFlags.mapped_directory = flags.sandbox_directory;
  hyperFlags.docker_socket = flags.docker_socket;
  hyperFlags.launcher_dir = flags.launcher_dir;

  // TODO(alexr): Remove this after the deprecation cycle (started in 0.29).
  hyperFlags.stop_timeout = flags.docker_stop_timeout;

  return hyperFlags;
}


Try<HyperContainerizerProcess::Container*>
HyperContainerizerProcess::Container::create(
    const ContainerID& id,
    const Option<TaskInfo>& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const UPID& slavePid,
    bool checkpoint,
    const Flags& flags)
{
  // Before we do anything else we first make sure the stdout/stderr
  // files exist and have the right file ownership.
  Try<Nothing> touch = os::touch(path::join(directory, "stdout"));

  if (touch.isError()) {
    return Error("Failed to touch 'stdout': " + touch.error());
  }

  touch = os::touch(path::join(directory, "stderr"));

  if (touch.isError()) {
    return Error("Failed to touch 'stderr': " + touch.error());
  }

  if (user.isSome()) {
    Try<Nothing> chown = os::chown(user.get(), directory);

    if (chown.isError()) {
      return Error("Failed to chown: " + chown.error());
    }
  }

  string hyperSymlinkPath = path::join(
      paths::getSlavePath(flags.work_dir, slaveId),
      HYPER_SYMLINK_DIRECTORY);

  Try<Nothing> mkdir = os::mkdir(hyperSymlinkPath);
  if (mkdir.isError()) {
    return Error("Unable to create symlink folder for hyper " +
                 hyperSymlinkPath + ": " + mkdir.error());
  }

  bool symlinked = false;
  string containerWorkdir = directory;
  // We need to symlink the sandbox directory if the directory
  // path has a colon, as Hyper CLI uses the colon as a seperator.
  if (strings::contains(directory, ":")) {
    containerWorkdir = path::join(hyperSymlinkPath, id.value());

    Try<Nothing> symlink = ::fs::symlink(directory, containerWorkdir);

    if (symlink.isError()) {
      return Error("Failed to symlink directory '" + directory +
                   "' to '" + containerWorkdir + "': " + symlink.error());
    }

    symlinked = true;
  }

  Option<ContainerInfo> containerInfo = None();
  Option<CommandInfo> commandInfo = None();
  Option<map<string, string>> environment = None();

  return new Container(
      id,
      taskInfo,
      executorInfo,
      containerWorkdir,
      user,
      slaveId,
      slavePid,
      checkpoint,
      symlinked,
      flags,
      commandInfo,
      containerInfo,
      environment);
}


Future<Nothing> HyperContainerizerProcess::fetch(
    const ContainerID& containerId,
    const SlaveID& slaveId)
{
  CHECK(containers_.contains(containerId));
  Container* container = containers_[containerId];

  return fetcher->fetch(
      containerId,
      container->command,
      container->directory,
      None(),
      slaveId,
      flags);
}


Future<Nothing> HyperContainerizerProcess::pull(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container is already destroyed");
  }

  Container* container = containers_[containerId];

  if (container->image.isNone()) {
    return Failure("Container image is None");
  }

  container->state = Container::PULLING;

  vector<string> argv = {
    HYPER_PATH,
    "pull",
    container->image.get()
  };

  Future<string> future = command::launch(HYPER_PATH, argv);

  containers_[containerId]->pull = future;

  return future.then(defer(self(), [=]() {
    VLOG(1) << "Hyper pull " << container->image.get() << " completed";
    return Nothing();
  }));
}


Try<Nothing> HyperContainerizerProcess::checkpoint(
    const ContainerID& containerId,
    pid_t pid)
{
  CHECK(containers_.contains(containerId));

  Container* container = containers_[containerId];

  container->executorPid = pid;

  if (container->checkpoint) {
    const string& path =
      slave::paths::getForkedPidPath(
          slave::paths::getMetaRootDir(flags.work_dir),
          container->slaveId,
          container->executor.framework_id(),
          container->executor.executor_id(),
          containerId);

    LOG(INFO) << "Checkpointing pid " << pid << " to '" << path <<  "'";

    return slave::state::checkpoint(path, stringify(pid));
  }

  return Nothing();
}


Future<Nothing> HyperContainerizer::recover(
    const SlaveID& slaveId,
    const list<ContainerState>& recoverables)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::recover,
      slaveId,
      recoverables);
}


Future<bool> HyperContainerizer::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const UPID& slavePid,
    bool checkpoint)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::launch,
      containerId,
      None(),
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint);
}


Future<bool> HyperContainerizer::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const UPID& slavePid,
    bool checkpoint)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::launch,
      containerId,
      taskInfo,
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint);
}


Future<Nothing> HyperContainerizer::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::update,
      containerId,
      resources,
      false);
}


Future<ResourceStatistics> HyperContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::usage,
      containerId);
}


Future<containerizer::Termination> HyperContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(
      process.get(),
      &HyperContainerizerProcess::wait,
      containerId);
}


void HyperContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(
      process.get(),
      &HyperContainerizerProcess::destroy,
      containerId, true);
}


Future<hashset<ContainerID>> HyperContainerizer::containers()
{
  return dispatch(process.get(), &HyperContainerizerProcess::containers);
}


Future<list<ContainerID>> HyperContainerizerProcess::_list(
    const Option<string>& prefix)
{
  vector<string> argv = {
    HYPER_PATH,
    "list"
  };

  return command::launch(HYPER_PATH, argv)
  .then(defer(self(), &Self::__list, prefix, lambda::_1));
}


Future<list<ContainerID>> HyperContainerizerProcess::__list(
    const Option<string>& prefix,
    const string& output)
{
  vector<string> lines = strings::tokenize(output, "\n");

  // Skip the header.
  CHECK(!lines.empty());
  lines.erase(lines.begin());

  list<ContainerID> containerIds;
  foreach (const string& line, lines) {
    vector<string> columns = strings::split(strings::trim(line), " ");

    CHECK(columns.size() > 1);
    string name = columns[1];
    if (prefix.isNone() || strings::startsWith(name, prefix.get())) {
      Option<ContainerID> containerId = parse(name);
      if (containerId.isSome()) {
        containerIds.push_back(containerId.get());
      }
    }
  }

  return containerIds;
}


Future<Nothing> HyperContainerizerProcess::recover(
    const SlaveID& slaveId,
    const list<ContainerState>& recoverables)
{
  LOG(INFO) << "Recovering Hyper containers";

  return _list(HYPER_NAME_PREFIX + slaveId.value())
    .then(defer(self(), &Self::_recover, slaveId, recoverables, lambda::_1));
}


Future<Nothing> HyperContainerizerProcess::_recover(
    const SlaveID& slaveId,
    const list<ContainerState>& recoverables,
    const list<ContainerID>& _containers)
{
  // Collection of pids that we've started reaping in order to
  // detect very unlikely duplicate scenario (see below).
  hashmap<ContainerID, pid_t> pids;

  foreach (const ContainerState& recoverable, recoverables) {
    const ExecutorInfo& executorInfo = recoverable.executor_info();
    const ContainerID& containerId = recoverable.container_id();
    const FrameworkID& frameworkId = executorInfo.framework_id();
    const ExecutorID& executorId = executorInfo.executor_id();

    if (!executorInfo.has_container() ||
      executorInfo.container().type() != ContainerInfo::CUSTOM ||
      !executorInfo.container().has_custom() ||
      executorInfo.container().custom().name() != CONTAINER_HYPER) {
      LOG(INFO) << "Skipping recovery of executor '" << executorId
                << "' of framework '" << frameworkId
                << "' because its executor is not marked as hyper "
                << "and the hyper container doesn't exist";
      LOG(INFO) << "Skipping recovery of executor '" << executorId
                << "' of framework " << frameworkId
                << " because it was not launched from hyper containerizer";
      continue;
    }

    LOG(INFO) << "Recovering container '" << containerId
              << "' for executor '" << executorId
              << "' of framework '" << frameworkId << "'";

    // Create and store a container.
    Container* container = new Container(containerId);
    containers_[containerId] = container;
    container->slaveId = slaveId;
    container->state = Container::RUNNING;

    pid_t pid = recoverable.pid();

    container->status.set(process::reap(pid));

    container->status.future().get()
      .onAny(defer(self(), &Self::reaped, containerId));

    if (pids.containsValue(pid)) {
      // This should (almost) never occur. There is the
      // possibility that a new executor is launched with the same
      // pid as one that just exited (highly unlikely) and the
      // slave dies after the new executor is launched but before
      // it hears about the termination of the earlier executor
      // (also unlikely).
      return Failure(
          "Detected duplicate pid " + stringify(pid) +
          " for container " + stringify(containerId));
    }

    pids.put(containerId, pid);

    container->directory = recoverable.directory();

    // Pass recovered containers to the container logger.
    // NOTE: The current implementation of the container logger only
    // outputs a warning and does not have any other consequences.
    // See `ContainerLogger::recover` for more information.
    logger->recover(executorInfo, container->directory)
      .onFailed(defer(self(), [executorInfo](const string& message) {
        LOG(WARNING) << "Container logger failed to recover executor '"
                     << executorInfo.executor_id() << "': "
                     << message;
      }));
  }

  // Stop orphan containers
  return __recover(_containers);
}


Future<Nothing> HyperContainerizerProcess::stop(const string& containerId)
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


Future<Nothing> HyperContainerizerProcess::__recover(
    const list<ContainerID>& _containers)
{
  list<ContainerID> containerIds;
  list<Future<Nothing>> futures;
  foreach (const ContainerID& containerId, _containers) {
    VLOG(1) << "Checking if Mesos container with ID '"
            << containerId << "' has been orphaned";

    // Check if we're watching an executor for this container ID and
    // if not, rm -f the Hyper container.
    if (!containers_.contains(containerId)) {
      futures.push_back(stop(stringify(containerId)));
      containerIds.push_back(containerId);
    }
  }

  return collect(futures)
    .then(defer(self(), [=]() -> Future<Nothing> {
      return Nothing();
    }));
}


Future<bool> HyperContainerizerProcess::launch(
    const ContainerID& containerId,
    const Option<TaskInfo>& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const UPID& slavePid,
    bool checkpoint)
{
  if (containers_.contains(containerId)) {
    return Failure("Container already started");
  }

  if (taskInfo.isNone()) {
    return Failure("TaskInfo is empty");
  }

  Option<ContainerInfo> containerInfo;

  if (taskInfo.get().has_container()) {
    containerInfo = taskInfo.get().container();
  } else if (executorInfo.has_container()) {
    containerInfo = executorInfo.container();
  }

  if (containerInfo.isNone()) {
    LOG(INFO) << "No container info found, skipping launch";
    return false;
  }

  if (containerInfo.get().type() != ContainerInfo::CUSTOM ||
    !containerInfo.get().has_custom() ||
    containerInfo.get().custom().name() != CONTAINER_HYPER) {
    LOG(INFO) << "Skipping non-hyper container:"
              << JSON::protobuf(containerInfo.get());
    return false;
  }

  Try<Container*> container = Container::create(
      containerId,
      taskInfo,
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint,
      flags);

  if (container.isError()) {
    return Failure("Failed to create container: " + container.error());
  }

  containers_[containerId] = container.get();

  LOG(INFO) << "Starting container '" << containerId
            << "' for task '" << taskInfo.get().task_id()
            << "' (and executor '" << executorInfo.executor_id()
            << "') of framework '" << executorInfo.framework_id() << "'";

  // Launching task by forking a subprocess to run executor.
  return container.get()->launch = fetch(containerId, slaveId)
    .then(defer(self(), [=]() { return pull(containerId); }))
    .then(defer(self(), [=]() { return launchExecutorProcess(containerId); }))
    .then(defer(self(), [=](pid_t pid) {
      return reapExecutor(containerId, pid);
    }));
}


Future<pid_t> HyperContainerizerProcess::launchExecutorProcess(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container is already destroyed");
  }

  Container* container = containers_[containerId];
  container->state = Container::RUNNING;

  // Prepare environment variables for the executor.
  map<string, string> environment = executorEnvironment(
      container->executor,
      container->directory,
      container->slaveId,
      container->slavePid,
      container->checkpoint,
      flags,
      false);

  // Include any enviroment variables from ExecutorInfo.
  foreach (const Environment::Variable& variable,
           container->executor.command().environment().variables()) {
    environment[variable.name()] = variable.value();
  }

  // Pass GLOG flag to the executor.
  const Option<string> glog = os::getenv("GLOG_v");
  if (glog.isSome()) {
    environment["GLOG_v"] = glog.get();
  }

  // FIXME(haosdent): wrap the command by hyper run.
  vector<string> argv;
  argv.push_back("mesos-executor");

  return logger->prepare(container->executor, container->directory)
    .then(defer(
        self(),
        [=](const ContainerLogger::SubprocessInfo& subprocessInfo)
          -> Future<pid_t> {
    // NOTE: The child process will be blocked until all hooks have been
    // executed.
    vector<Subprocess::Hook> parentHooks;

    // NOTE: Currently we don't care about the order of the hooks, as
    // both hooks are independent.

    // A hook that is executed in the parent process. It attempts to checkpoint
    // the process pid.
    //
    // NOTE:
    // - The child process is blocked by the hook infrastructure while
    //   these hooks are executed.
    // - It is safe to bind `this`, as hooks are executed immediately
    //   in a `subprocess` call.
    // - If `checkpoiont` returns an Error, the child process will be killed.
    parentHooks.emplace_back(Subprocess::Hook(lambda::bind(
        &HyperContainerizerProcess::checkpoint,
        this,
        containerId,
        lambda::_1)));

    // Construct the mesos-executor using the "name" we gave the
    // container (to distinguish it from Hyper containers not created
    // by Mesos).
    Try<Subprocess> s = subprocess(
        path::join(flags.launcher_dir, "mesos-hyper-executor"),
        argv,
        Subprocess::PIPE(),
        (local ? Subprocess::FD(STDOUT_FILENO)
               : Subprocess::IO(subprocessInfo.out)),
        (local ? Subprocess::FD(STDERR_FILENO)
               : Subprocess::IO(subprocessInfo.err)),
        SETSID,
        hyperFlags(flags, container->name(), container->directory),
        environment,
        None(),
        parentHooks,
        container->directory);

    if (s.isError()) {
      return Failure("Failed to fork executor: " + s.error());
    }

    return s.get().pid();
  }));
}


Future<bool> HyperContainerizerProcess::reapExecutor(
    const ContainerID& containerId,
    pid_t pid)
{
  // After we do Hyper::run we shouldn't remove a container until
  // after we set 'status', which we do in this function.
  CHECK(containers_.contains(containerId));

  Container* container = containers_[containerId];

  // And finally watch for when the container gets reaped.
  container->status.set(process::reap(pid));

  container->status.future().get()
    .onAny(defer(self(), &Self::reaped, containerId));

  return true;
}


Future<Nothing> HyperContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& _resources,
    bool force)
{
  if (!containers_.contains(containerId)) {
    LOG(WARNING) << "Ignoring updating unknown container: "
                 << containerId;
    return Nothing();
  }

  Container* container = containers_[containerId];

  if (container->state == Container::DESTROYING)  {
    LOG(INFO) << "Ignoring updating container '" << containerId
              << "' that is being destroyed";
    return Nothing();
  }

  if (container->resources == _resources && !force) {
    LOG(INFO) << "Ignoring updating container '" << containerId
              << "' with resources passed to update is identical to "
              << "existing resources";
    return Nothing();
  }

  // Store the resources for usage().
  container->resources = _resources;

  return Nothing();
}


Future<ResourceStatistics> HyperContainerizerProcess::usage(
    const ContainerID& containerId)
{
  return Failure("Does not support usage()");
}


Future<containerizer::Termination> HyperContainerizerProcess::wait(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Unknown container: " + stringify(containerId));
  }

  return containers_[containerId]->termination.future();
}


void HyperContainerizerProcess::destroy(
    const ContainerID& containerId,
    bool killed)
{
  if (!containers_.contains(containerId)) {
    LOG(WARNING) << "Ignoring destroy of unknown container: " << containerId;
    return;
  }

  Container* container = containers_[containerId];

  if (container->launch.isFailed()) {
    VLOG(1) << "Container '" << containerId << "' launch failed";

    // This means we failed to launch the container and we're trying to
    // cleanup.
    CHECK_PENDING(container->status.future());

    // NOTE: The launch error message will be retrieved by the slave
    // and properly set in the corresponding status update.
    container->termination.set(containerizer::Termination());

    containers_.erase(containerId);
    delete container;

    return;
  }

  if (container->state == Container::DESTROYING) {
    // Destroy has already been initiated.
    return;
  }

  LOG(INFO) << "Destroying container '" << containerId << "'";

  // It's possible that destroy is getting called before
  // HyperContainerizer::launch has completed (i.e., after we've
  // returned a future but before we've completed the fetching of the
  // URIs, or the Hyper::run, or the wait, etc.).
  //
  // If we're FETCHING, we want to stop the fetching and then
  // cleanup. Note, we need to make sure that we deal with the race
  // with trying to terminate the fetcher so that even if the fetcher
  // returns successfully we won't try to do a Hyper::run.
  //
  // If we're PULLING, we want to terminate the 'hyper pull' and then
  // cleanup. Just as above, we'll need to deal with the race with
  // 'hyper pull' returning successfully.
  //
  // If we're RUNNING, we want to wait for the status to get set, then
  // do a Hyper::kill, then wait for the status to complete, then
  // cleanup.

  if (container->state == Container::FETCHING) {
    LOG(INFO) << "Destroying Container '"
              << containerId << "' in FETCHING state";

    fetcher->kill(containerId);

    containerizer::Termination termination;
    termination.set_message("Container destroyed while fetching");
    container->termination.set(termination);

    // Even if the fetch succeeded just before we did the killtree,
    // removing the container here means that we won't proceed with
    // the Hyper::run.
    containers_.erase(containerId);
    delete container;

    return;
  }

  if (container->state == Container::PULLING) {
    LOG(INFO) << "Destroying Container '"
              << containerId << "' in PULLING state";

    container->pull.discard();

    containerizer::Termination termination;
    termination.set_message("Container destroyed while pulling image");
    container->termination.set(termination);

    containers_.erase(containerId);
    delete container;

    return;
  }

  CHECK(container->state == Container::RUNNING);

  container->state = Container::DESTROYING;

  if (killed && container->executorPid.isSome()) {
    LOG(INFO) << "Sending SIGTERM to executor with pid: "
              << container->executorPid.get();
    // We need to clean up the executor as the executor might not have
    // received run task due to a failed containerizer update.
    // We also kill the executor first since container->status below
    // is waiting for the executor to finish.
    Try<list<os::ProcessTree>> kill =
      os::killtree(container->executorPid.get(), SIGTERM);

    if (kill.isError()) {
      // Ignoring the error from killing executor as it can already
      // have exited.
      VLOG(1) << "Ignoring error when killing executor pid "
              << container->executorPid.get() << " in destroy, error: "
              << kill.error();
    }
  }

  // Otherwise, wait for Hyper::run to succeed, in which case we'll
  // continue in _destroy (calling Hyper::kill) or for Hyper::run to
  // fail, in which case we'll re-execute this function and cleanup
  // above.
  container->status.future()
    .onAny(defer(self(), &Self::_destroy, containerId, killed));
}


void HyperContainerizerProcess::_destroy(
    const ContainerID& containerId,
    bool killed)
{
  CHECK(containers_.contains(containerId));

  Container* container = containers_[containerId];

  CHECK(container->state == Container::DESTROYING);

  // Do a 'hyper stop' which we'll then find out about in '_destroy'
  // after we've reaped either the container's root process (in the
  // event that we had just launched a container for an executor) or
  // the mesos-executor (in the case we launched a container
  // for a task).
  LOG(INFO) << "Running hyper stop on container '" << containerId << "'";

  if (killed) {
    // TODO(alexr): After the deprecation cycle (started in 0.29.0), update
    // this to omit the timeout. Graceful shutdown of the container is not
    // a containerizer responsibility; it is the responsibility of the agent
    // in co-operation with the executor. Once `destroy()` is called, the
    // container should be destroyed forcefully.
    stop(container->name())
      .onAny(defer(self(), &Self::__destroy, containerId, killed, lambda::_1));
  } else {
    __destroy(containerId, killed, Nothing());
  }
}


void HyperContainerizerProcess::__destroy(
    const ContainerID& containerId,
    bool killed,
    const Future<Nothing>& kill)
{
  CHECK(containers_.contains(containerId));

  Container* container = containers_[containerId];

  if (!kill.isReady() && !container->status.future().isReady()) {
    container->termination.fail(
        "Failed to kill the Hyper container: " +
        (kill.isFailed() ? kill.failure() : "discarded future"));

    containers_.erase(containerId);

    delay(
      HYPER_REMOVE_DELAY,
      self(),
      &Self::remove,
      container->name());

    delete container;

    return;
  }

  // Status must be ready since we did a Hyper::kill.
  CHECK_READY(container->status.future());

  container->status.future().get()
    .onAny(defer(self(), &Self::___destroy, containerId, killed, lambda::_1));
}


void HyperContainerizerProcess::___destroy(
    const ContainerID& containerId,
    bool killed,
    const Future<Option<int>>& status)
{
  CHECK(containers_.contains(containerId));

  Container* container = containers_[containerId];

  containerizer::Termination termination;

  if (status.isReady() && status.get().isSome()) {
    termination.set_status(status.get().get());
  }

  termination.set_message(
      killed ? "Container killed" : "Container terminated");

  container->termination.set(termination);

  containers_.erase(containerId);

  delay(
    HYPER_REMOVE_DELAY,
    self(),
    &Self::remove,
    container->name());

  delete container;
}


Future<hashset<ContainerID>> HyperContainerizerProcess::containers()
{
  return containers_.keys();
}


void HyperContainerizerProcess::reaped(const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return;
  }

  LOG(INFO) << "Executor for container '" << containerId << "' has exited";

  // The executor has exited so destroy the container.
  destroy(containerId, false);
}


void HyperContainerizerProcess::remove(
    const string& containerName)
{
  vector<string> argv = {
    HYPER_PATH,
    "rm",
    containerName
  };

  command::launch(HYPER_PATH, argv);
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
