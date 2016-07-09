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

#include <stout/error.hpp>
#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include "linux/cgroups.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/subsystem.hpp"

using mesos::slave::ContainerLimitation;

using process::Failure;
using process::Future;
using process::Owned;
using process::PID;

using std::string;

namespace mesos {
namespace internal {
namespace slave {

Try<Owned<Subsystem>> Subsystem::create(
    const Flags& _flags,
    const string& _name,
    const string& _hierarchy)
{
  Owned<Subsystem> subsystem;

  if (_name == CGROUP_SUBSYSTEM_CPU_NAME) {
    subsystem = Owned<Subsystem>(new CpuSubsystem(_flags, _hierarchy));
  } else {
    return Error("Unknown subsystem '" + _name + "'");
  }

  Try<Nothing> load = subsystem->load();
  if (load.isError()) {
    return Error("Failed to load subsystem '" + _name + "': " +
                 load.error());
  }

  return subsystem;
}


Subsystem::Subsystem(
    const Flags& _flags,
    const string& _hierarchy)
  : flags(_flags),
    hierarchy(_hierarchy) {}


Try<Nothing> Subsystem::load()
{
  return Nothing();
}


Future<Nothing> Subsystem::recover(const ContainerID& containerId)
{
  return Nothing();
}


Future<Nothing> Subsystem::prepare(const ContainerID& containerId)
{
  return Nothing();
}


Future<Nothing> Subsystem::isolate(const ContainerID& containerId, pid_t pid)
{
  return Nothing();
}


Future<Nothing> Subsystem::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return Nothing();
}


Future<ResourceStatistics> Subsystem::usage(const ContainerID& containerId)
{
  return ResourceStatistics();
}


Future<ContainerStatus> Subsystem::status(const ContainerID& containerId)
{
  return ContainerStatus();
}


Future<Nothing> Subsystem::cleanup(const ContainerID& containerId)
{
  return Nothing();
}


CpuSubsystem::CpuSubsystem(
    const Flags& _flags,
    const string& _hierarchy)
  : ProcessBase(process::ID::generate("cgroups-cpu-subsystem")),
    Subsystem(_flags, _hierarchy) {}


Try<Nothing> CpuSubsystem::load()
{
  if (flags.cgroups_enable_cfs) {
    Try<bool> exists = cgroups::exists(
        hierarchy,
        flags.cgroups_root,
        "cpu.cfs_quota_us");

    if (exists.isError() || !exists.get()) {
      return Error(
          "Failed to find 'cpu.cfs_quota_us'. Your kernel might be too old to "
          "use the CFS cgroups feature.");
    }
  }

  return Nothing();
}


Future<Nothing> CpuSubsystem::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  if (resources.cpus().isNone()) {
    return Failure(
        "Failed to update subsystem '" + name() + "': No cpus resource given");
  }

  double cpus = resources.cpus().get();

  // Always set cpu.shares.
  uint64_t shares;

  if (flags.revocable_cpu_low_priority &&
      resources.revocable().cpus().isSome()) {
    shares = std::max(
        (uint64_t) (CPU_SHARES_PER_CPU_REVOCABLE * cpus),
        MIN_CPU_SHARES);
  } else {
    shares = std::max(
        (uint64_t) (CPU_SHARES_PER_CPU * cpus),
        MIN_CPU_SHARES);
  }

  Try<Nothing> write = cgroups::cpu::shares(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()),
      shares);

  if (write.isError()) {
    return Failure("Failed to update 'cpu.shares': " + write.error());
  }

  LOG(INFO) << "Updated 'cpu.shares' to " << shares
            << " (cpus " << cpus << ")"
            << " for container '" << containerId << "'";

  // Set cfs quota if enabled.
  if (flags.cgroups_enable_cfs) {
    write = cgroups::cpu::cfs_period_us(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        CPU_CFS_PERIOD);

    if (write.isError()) {
      return Failure("Failed to update 'cpu.cfs_period_us': " + write.error());
    }

    Duration quota = std::max(CPU_CFS_PERIOD * cpus, MIN_CPU_CFS_QUOTA);

    write = cgroups::cpu::cfs_quota_us(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        quota);

    if (write.isError()) {
      return Failure("Failed to update 'cpu.cfs_quota_us': " + write.error());
    }

    LOG(INFO) << "Updated 'cpu.cfs_period_us' to " << CPU_CFS_PERIOD
              << " and 'cpu.cfs_quota_us' to " << quota
              << " (cpus " << cpus << ")"
              << " for container '" << containerId << "'";
  }

  return Nothing();
}


Future<ResourceStatistics> CpuSubsystem::usage(const ContainerID& containerId)
{
  ResourceStatistics result;

  // Add the cpu.stat information only if CFS is enabled.
  if (flags.cgroups_enable_cfs) {
    Try<hashmap<string, uint64_t>> stat = cgroups::stat(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        "cpu.stat");

    if (stat.isError()) {
      return Failure("Failed to read 'cpu.stat': " + stat.error());
    }

    Option<uint64_t> nr_periods = stat.get().get("nr_periods");
    if (nr_periods.isSome()) {
      result.set_cpus_nr_periods(nr_periods.get());
    }

    Option<uint64_t> nr_throttled = stat.get().get("nr_throttled");
    if (nr_throttled.isSome()) {
      result.set_cpus_nr_throttled(nr_throttled.get());
    }

    Option<uint64_t> throttled_time = stat.get().get("throttled_time");
    if (throttled_time.isSome()) {
      result.set_cpus_throttled_time_secs(
          Nanoseconds(throttled_time.get()).secs());
    }
  }

  return result;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
