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

#ifndef __CGROUPS_ISOLATOR_SUBSYSTEM_HPP__
#define __CGROUPS_ISOLATOR_SUBSYSTEM_HPP__

#include <list>
#include <string>
#include <tuple>
#include <vector>

#include <mesos/resources.hpp>

#include <mesos/slave/isolator.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>

#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "linux/cgroups.hpp"

#include "slave/flags.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/constants.hpp"
#include "slave/containerizer/mesos/isolators/cgroups/net_cls.hpp"

namespace mesos {
namespace internal {
namespace slave {

/**
 * An abstraction for cgroups subsystem.
 */
class Subsystem : public process::Process<Subsystem>
{
public:
  /**
   * Attempts to create a specific `Subsystem` object that will contain specific
   * information associated with container.
   *
   * @param flags `Flags` used to launch the agent.
   * @param name The name of cgroups subsystem.
   * @param hierarchy The hierarchy path of cgroups subsystem.
   * @return A specific `Subsystem` object or an error if `create` fails.
   */
  static Try<process::Owned<Subsystem>> create(
      const Flags& _flags,
      const std::string& _name,
      const std::string& _hierarchy);

  virtual ~Subsystem() {}

  /**
   * The cgroups subsystem name of this `Subsystem` object.
   *
   * @return The cgroups subsystem name.
   */
  virtual std::string name() const = 0;

  /**
   * Initialize necessary variables.
   *
   * @param _notifyCallback A callback the `Subsystem` uses to notify the
   *     container is impacted by current `Subsystem` resource limitation.
   */
  void init(
      const lambda::function<
          void(const ContainerID&,
               const mesos::slave::ContainerLimitation&)>& _notifyCallback);

  /**
   * Recover the cgroups subsystem for the associated container.
   *
   * @param containerId The target containerId.
   * @return Nothing or an error if `recover` fails.
   */
  virtual process::Future<Nothing> recover(const ContainerID& containerId);

  /**
   * Prepare the cgroups subsystem for the associated container.
   *
   * @param containerId The target containerId.
   * @return Nothing or an error if `prepare` fails.
   */
  virtual process::Future<Nothing> prepare(const ContainerID& containerId);

  /**
   * Isolate the associated container to cgroups subsystem.
   *
   * @param containerId The target containerId.
   * @param pid The process id of container.
   * @return Nothing or an error if `isolate` fails.
   */
  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  /**
   * Update resources allocated to the associated container in this cgroups
   * subsystem.
   *
   * @param containerId The target containerId.
   * @param resources The resources need to update.
   * @return Nothing or an error if `update` fails.
   */
  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  /**
   * Gather resource usage statistics of the cgroups subsystem for the
   * associated container.
   *
   * @param containerId The target containerId.
   * @return The resource usage statistics or an error if gather statistics
   *     fails.
   */
  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  /**
   * Get the run-time status of cgroups subsystem specific properties associated
   * with the container.
   *
   * @param containerId The target containerId.
   * @return The container status or an error if get fails.
   */
  virtual process::Future<ContainerStatus> status(
      const ContainerID& containerId);

  /**
   * Clean up the cgroups subsystem for the associated container. It will be
   * called when destruction to ensure everyting be cleanup.
   *
   * @param containerId The target containerId.
   * @return Nothing or an error if `cleanup` fails.
   */
  virtual process::Future<Nothing> cleanup(const ContainerID& containerId);

protected:
  Subsystem(const Flags& _flags, const std::string& _hierarchy);

  /**
   * Load cgroups subsystem. This method checks and prepares the environment for
   * cgroups subsystem.
   *
   * @return Nothing or an error if `load` fails.
   */
  virtual Try<Nothing> load();

  /**
   * `Flags` used to launch the agent.
   */
  const Flags flags;

  /**
   * The callback that is uses to notify the isolator the container is impacted
   * by a resource limitation.
   */
  lambda::function<
      void(const ContainerID&,
           const mesos::slave::ContainerLimitation&)> notifyCallback;

  /**
   * The hierarchy path of cgroups subsystem.
   */
  const std::string hierarchy;
};


/**
 * Represent cgroups cpu subsystem.
 */
class CpuSubsystem : public Subsystem
{
public:
  CpuSubsystem(const Flags& _flags, const std::string& _hierarchy);

  virtual ~CpuSubsystem() {}

  virtual std::string name() const
  {
    return CGROUP_SUBSYSTEM_CPU_NAME;
  };

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

protected:
  virtual Try<Nothing> load();
};


/**
 * Represent cgroups cpuacct subsystem.
 */
class CpuacctSubsystem : public Subsystem
{
public:
  CpuacctSubsystem(const Flags& _flags, const std::string& _hierarchy);

  virtual ~CpuacctSubsystem() {}

  virtual std::string name() const
  {
    return CGROUP_SUBSYSTEM_CPUACCT_NAME;
  }

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);
};


/**
 * Represent cgroups memory subsystem.
 */
class MemorySubsystem : public Subsystem
{
public:
  MemorySubsystem(const Flags& _flags, const std::string& _hierarchy);

  virtual ~MemorySubsystem() {}

  virtual std::string name() const
  {
    return CGROUP_SUBSYSTEM_MEMORY_NAME;
  }

  virtual process::Future<Nothing> prepare(const ContainerID& containerId);

  virtual process::Future<Nothing> recover(const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(const ContainerID& containerId);

protected:
  virtual Try<Nothing> load();

private:
  struct Info {
    // Used to cancel the OOM listening.
    process::Future<Nothing> oomNotifier;

    // If already set the hard limit before.
    bool updatedLimit;

    hashmap<
        cgroups::memory::pressure::Level,
        process::Owned<cgroups::memory::pressure::Counter>> pressureCounters;
  };

  inline const std::vector<cgroups::memory::pressure::Level> levels() {
    return {
      cgroups::memory::pressure::Level::LOW,
      cgroups::memory::pressure::Level::MEDIUM,
      cgroups::memory::pressure::Level::CRITICAL
    };
  }

  process::Future<ResourceStatistics> _usage(
      const ContainerID& containerId,
      ResourceStatistics result,
      const std::list<cgroups::memory::pressure::Level>& levels,
      const std::list<process::Future<uint64_t>>& values);

  void oomListen(const ContainerID& containerId);

  void oomWaited(
      const process::Future<Nothing>& future,
      const ContainerID& containerId);

  void oom(const ContainerID& containerId);

  void pressureListen(const ContainerID& containerId);

  /**
   * Store cgroups associated information for container.
   */
  hashmap<ContainerID, process::Owned<Info>> infos;
};


/**
 * Represent cgroups net_cls subsystem.
 */
class NetClsSubsystem : public Subsystem
{
public:
  NetClsSubsystem(const Flags& _flags, const std::string& _hierarchy);

  virtual ~NetClsSubsystem() {}

  virtual std::string name() const
  {
    return CGROUP_SUBSYSTEM_NET_CLS_NAME;
  }

  virtual process::Future<Nothing> recover(const ContainerID& containerId);

  virtual process::Future<Nothing> prepare(const ContainerID& containerId);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId, pid_t pid);

  virtual process::Future<ContainerStatus> status(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(const ContainerID& containerId);

protected:
  virtual Try<Nothing> load();

private:
  struct Info
  {
    Info() {}

    Info(const NetClsHandle &_handle)
      : handle(_handle) {}

    const Option<NetClsHandle> handle;
  };

  Result<NetClsHandle> recoverHandle(
      const std::string& hierarchy,
      const std::string& cgroup);

  Option<NetClsHandleManager> handleManager;

  /**
   * Store cgroups associated information for container.
   */
  hashmap<ContainerID, process::Owned<Info>> infos;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __CGROUPS_ISOLATOR_SUBSYSTEM_HPP__
