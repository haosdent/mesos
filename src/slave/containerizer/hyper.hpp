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

#ifndef __HYPER_CONTAINERIZER_HPP__
#define __HYPER_CONTAINERIZER_HPP__

#include <mesos/slave/container_logger.hpp>

#include <mesos/slave/containerizer/containerizer.hpp>

#include <process/owned.hpp>
#include <process/shared.hpp>

#include <stout/flags.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>

#include "hyper/executor.hpp"

#include "slave/containerizer/containerizer_utils.hpp"
#include "slave/containerizer/fetcher.hpp"

namespace mesos {
namespace internal {
namespace slave {

extern const std::string CONTAINER_HYPER;

// Prefix used to name Hyper containers in order to distinguish those
// created by Mesos from those created manually.
extern const std::string HYPER_NAME_PREFIX;

// Seperator used to compose hyper container name, which is made up
// of slave ID and container ID.
extern const std::string HYPER_NAME_SEPERATOR;

// Directory that stores all the symlinked sandboxes that is mapped
// into Hyper containers. This is a relative directory that will
// joined with the slave path. Only sandbox paths that contains a
// colon will be symlinked due to the limiitation of the Hyper CLI.
extern const std::string HYPER_SYMLINK_DIRECTORY;

extern const std::string HYPER_PATH;


// Forward declaration.
class HyperContainerizerProcess;


class HyperContainerizer : public mesos::slave::Containerizer
{
public:
  static Try<mesos::slave::Containerizer*> create(const Parameters& parameters);

  HyperContainerizer(
      const Flags& flags,
      bool local,
      const process::Owned<Fetcher>& fetcher,
      const process::Owned<mesos::slave::ContainerLogger>& logger);

  HyperContainerizer(
      const process::Owned<HyperContainerizerProcess>& _process);

  virtual ~HyperContainerizer();

  virtual process::Future<Nothing> recover(
      const SlaveID& slaveId,
      const std::list<mesos::slave::ContainerState>& recoverables);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::UPID& slavePid,
      bool checkpoint);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::UPID& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();

private:
  process::Owned<HyperContainerizerProcess> process;
};



class HyperContainerizerProcess
  : public process::Process<HyperContainerizerProcess>
{
public:
  HyperContainerizerProcess(
      const Flags& _flags,
      bool _local,
      const process::Owned<Fetcher> _fetcher,
      const process::Owned<mesos::slave::ContainerLogger>& _logger)
    : flags(_flags),
      local(_local),
      fetcher(_fetcher),
      logger(_logger) {}

  virtual process::Future<Nothing> recover(
      const SlaveID& slaveId,
      const std::list<mesos::slave::ContainerState>& recoverables);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const Option<TaskInfo>& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::UPID& slavePid,
      bool checkpoint);

  // force = true causes the containerizer to update the resources
  // for the container, even if they match what it has cached.
  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources,
      bool force);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(
      const ContainerID& containerId,
      bool killed = true); // process is either killed or reaped.

  virtual process::Future<Nothing> fetch(
      const ContainerID& containerId,
      const SlaveID& slaveId);

  virtual process::Future<Nothing> pull(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();

private:
  // Continuations and helpers.
  process::Future<Nothing> _fetch(
      const ContainerID& containerId,
      const Option<int>& status);

  Try<Nothing> checkpoint(
      const ContainerID& containerId,
      pid_t pid);

  process::Future<std::list<ContainerID>> _list(
      const Option<std::string>& prefix);

  process::Future<std::list<ContainerID>> __list(
      const Option<std::string>& prefix,
      const std::string& output);

  process::Future<Nothing> stop(const std::string& containerId);

  process::Future<Nothing> _recover(
      const SlaveID& slaveId,
      const std::list<mesos::slave::ContainerState>& recoverables,
      const std::list<ContainerID>& containers);

  process::Future<Nothing> __recover(
      const std::list<ContainerID>& containers);

  // Starts the hyper executor with a subprocess.
  process::Future<pid_t> launchExecutorProcess(
      const ContainerID& containerId);

  // Reaps on the executor pid.
  process::Future<bool> reapExecutor(
      const ContainerID& containerId,
      pid_t pid);

  void _destroy(
      const ContainerID& containerId,
      bool killed);

  void __destroy(
      const ContainerID& containerId,
      bool killed,
      const process::Future<Nothing>& future);

  void ___destroy(
      const ContainerID& containerId,
      bool killed,
      const process::Future<Option<int>>& status);

  process::Future<Nothing> __update(
      const ContainerID& containerId,
      const Resources& resources,
      pid_t pid);

  // Call back for when the executor exits. This will trigger
  // container destroy.
  void reaped(const ContainerID& containerId);

  // Removes the hyper container.
  void remove(const std::string& containerName);

  const Flags flags;

  bool local;

  process::Owned<Fetcher> fetcher;

  process::Owned<mesos::slave::ContainerLogger> logger;

  struct Container
  {
    static Try<Container*> create(
        const ContainerID& id,
        const Option<TaskInfo>& taskInfo,
        const ExecutorInfo& executorInfo,
        const std::string& directory,
        const Option<std::string>& user,
        const SlaveID& slaveId,
        const process::UPID& slavePid,
        bool checkpoint,
        const Flags& flags);

    static std::string name(const SlaveID& slaveId, const std::string& id)
    {
      return HYPER_NAME_PREFIX + slaveId.value() + HYPER_NAME_SEPERATOR +
        stringify(id);
    }

    Container(const ContainerID& id)
      : state(FETCHING), id(id) {}

    Container(const ContainerID& id,
              const Option<TaskInfo>& taskInfo,
              const ExecutorInfo& executorInfo,
              const std::string& directory,
              const Option<std::string>& user,
              const SlaveID& slaveId,
              const process::UPID& slavePid,
              bool checkpoint,
              bool symlinked,
              const Flags& flags,
              const Option<CommandInfo>& _command,
              const Option<ContainerInfo>& _container,
              const Option<std::map<std::string, std::string>>& _environment)
      : state(FETCHING),
        id(id),
        task(taskInfo),
        executor(executorInfo),
        directory(directory),
        user(user),
        slaveId(slaveId),
        slavePid(slavePid),
        checkpoint(checkpoint),
        symlinked(symlinked),
        flags(flags)
    {
      // NOTE: The task's resources are included in the executor's
      // resources in order to make sure when launching the executor
      // that it has non-zero resources in the event the executor was
      // not actually given any resources by the framework
      // originally. See Framework::launchExecutor in slave.cpp. We
      // check that this is indeed the case here to protect ourselves
      // from when/if this changes in the future (but it's not a
      // perfect check because an executor might always have a subset
      // of it's resources that match a task, nevertheless, it's
      // better than nothing).
      resources = executor.resources();

      if (task.isSome()) {
        CHECK(resources.contains(task.get().resources()));
      }

      if (_command.isSome()) {
        command = _command.get();
      } else if (task.isSome()) {
        command = task.get().command();
      } else {
        command = executor.command();
      }

      if (_container.isSome()) {
        container = _container.get();
      } else if (task.isSome()) {
        container = task.get().container();
      } else {
        container = executor.container();
      }

      if (_environment.isSome()) {
        environment = _environment.get();
      } else {
        environment = executorEnvironment(
            executor,
            directory,
            slaveId,
            slavePid,
            checkpoint,
            flags,
            false);
      }

      ContainerInfo::CustomInfo customInfo;
      if (task.isSome()) {
         customInfo = task.get().container().custom();
      } else {
        customInfo = executor.container().custom();
      }

      foreach (const Parameter& parameter, customInfo.parameters()) {
        if (parameter.key() == "image") {
          image = parameter.value();
        }
      }
    }

    ~Container()
    {
      if (symlinked) {
        // The sandbox directory is a symlink, remove it at container
        // destroy.
        os::rm(directory);
      }
    }

    std::string name()
    {
      return name(slaveId, stringify(id));
    }

    // The HyperContainerier needs to be able to properly clean up
    // Hyper containers, regardless of when they are destroyed. For
    // example, if a container gets destroyed while we are fetching,
    // we need to not keep running the fetch, nor should we try and
    // start the Hyper container. For this reason, we've split out
    // the states into:
    //
    //     FETCHING
    //     PULLING
    //     MOUNTING
    //     RUNNING
    //     DESTROYING
    //
    // In particular, we made 'PULLING' be it's own state so that we
    // could easily destroy and cleanup when a user initiated pulling
    // a really big image but we timeout due to the executor
    // registration timeout. Since we curently have no way to discard
    // a Hyper::run, we needed to explicitely do the pull (which is
    // the part that takes the longest) so that we can also explicitly
    // kill it when asked. Once the functions at Hyper::* get support
    // for discarding, then we won't need to make pull be it's own
    // state anymore, although it doesn't hurt since it gives us
    // better error messages.
    enum State
    {
      FETCHING = 1,
      PULLING = 2,
      MOUNTING = 3,
      RUNNING = 4,
      DESTROYING = 5
    } state;

    const ContainerID id;
    const Option<TaskInfo> task;
    const ExecutorInfo executor;
    ContainerInfo container;
    CommandInfo command;
    std::map<std::string, std::string> environment;

    // The sandbox directory for the container. This holds the
    // symlinked path if symlinked boolean is true.
    std::string directory;

    const Option<std::string> user;
    SlaveID slaveId;
    const process::UPID slavePid;
    bool checkpoint;
    bool symlinked;
    const Flags flags;

    // Promise for future returned from wait().
    process::Promise<containerizer::Termination> termination;

    // Exit status of executor or container (depending on whether or
    // not we used the command executor). Represented as a promise so
    // that destroying can chain with it being set.
    process::Promise<process::Future<Option<int>>> status;

    // Future that tells us the return value of last launch stage (fetch, pull,
    // run, etc).
    process::Future<bool> launch;

    // We keep track of the resources for each container so we can set
    // the ResourceStatistics limits in usage(). Note that this is
    // different than just what we might get from TaskInfo::resources
    // or ExecutorInfo::resources because they can change dynamically.
    Resources resources;

    // The hyper pull future is stored so we can discard when
    // destroy is called while hyper is pulling the image.
    process::Future<std::string> pull;

    // Once the container is running, this saves the pid of the
    // running container.
    Option<pid_t> pid;

    // The executor pid that was forked to wait on the running
    // container. This is stored so we can clean up the executor
    // on destroy.
    Option<pid_t> executorPid;

    // The image used to launch Hyper container.
    Option<std::string> image;
  };

  hashmap<ContainerID, Container*> containers_;
};


} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __HYPER_CONTAINERIZER_HPP__
