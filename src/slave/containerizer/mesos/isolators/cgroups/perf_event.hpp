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

#ifndef __PERF_EVENT_ISOLATOR_HPP__
#define __PERF_EVENT_ISOLATOR_HPP__

#include <set>

#include <process/id.hpp>
#include <process/owned.hpp>
#include <process/time.hpp>

#include <stout/hashmap.hpp>
#include <stout/nothing.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/mesos/isolator.hpp"

namespace mesos {
namespace internal {
namespace slave {

class PerfEventHandleManager : public process::Process<PerfEventHandleManager>
{
public:
  PerfEventHandleManager(
      const Flags& _flags,
      const std::set<std::string>& _events);

  virtual void addCgroup(const std::string& cgroup);

  virtual void removeCgroup(const std::string& cgroup);

  virtual Option<PerfStatistics> getStatistics(const std::string& cgroup);

protected:
  virtual void initialize();

private:
  void sample();

  void _sample(
      const process::Time& next,
      const process::Future<hashmap<std::string, PerfStatistics>>& _statistics);

  Flags flags;

  // Set of events to sample.
  std::set<std::string> events;

  std::set<std::string> cgroups;

  hashmap<std::string, PerfStatistics> statistics;
};


class CgroupsPerfEventIsolatorProcess : public MesosIsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(const Flags& flags);

  virtual ~CgroupsPerfEventIsolatorProcess();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ContainerState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<Option<mesos::slave::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const mesos::slave::ContainerConfig& containerConfig);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  CgroupsPerfEventIsolatorProcess(
      const Flags& _flags,
      const std::string& _hierarchy,
      const std::set<std::string>& _events);

  virtual process::Future<Nothing> _cleanup(const ContainerID& containerId);

  struct Info
  {
    Info(const ContainerID& _containerId, const std::string& _cgroup)
      : containerId(_containerId), cgroup(_cgroup)
    {
      // Ensure the initial statistics include the required fields.
      // Note the duration is set to zero to indicate no sampling has
      // taken place. This empty sample will be returned from usage()
      // until the first true sample is obtained.
      statistics.set_timestamp(process::Clock::now().secs());
      statistics.set_duration(Seconds(0).secs());
    }

    const ContainerID containerId;
    const std::string cgroup;
    PerfStatistics statistics;
  };

  const Flags flags;

  // The path to the cgroups subsystem hierarchy root.
  const std::string hierarchy;

  // TODO(jieyu): Use Owned<Info>.
  hashmap<ContainerID, Info*> infos;

  process::Owned<PerfEventHandleManager> handleManager;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __PERF_EVENT_ISOLATOR_HPP__
