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

#include <set>
#include <sstream>

#include <process/collect.hpp>
#include <process/defer.hpp>

#include <stout/error.hpp>
#include <stout/interval.hpp>
#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include "common/protobuf_utils.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/subsystem.hpp"

using cgroups::memory::pressure::Counter;
using cgroups::memory::pressure::Level;

using mesos::slave::ContainerLimitation;

using process::Failure;
using process::Future;
using process::Owned;
using process::PID;

using std::list;
using std::ostringstream;
using std::set;
using std::string;
using std::tie;
using std::tuple;
using std::vector;

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
  } else if (_name == CGROUP_SUBSYSTEM_CPUACCT_NAME) {
    subsystem = Owned<Subsystem>(new CpuacctSubsystem(_flags, _hierarchy));
  } else if (_name == CGROUP_SUBSYSTEM_MEMORY_NAME) {
    subsystem = Owned<Subsystem>(new MemorySubsystem(_flags, _hierarchy));
  } else if (_name == CGROUP_SUBSYSTEM_NET_CLS_NAME) {
    subsystem = Owned<Subsystem>(new NetClsSubsystem(_flags, _hierarchy));
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


void Subsystem::init(
    const lambda::function<
        void(const ContainerID&, const ContainerLimitation&)>& _notifyCallback)
{
  notifyCallback = _notifyCallback;
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


CpuacctSubsystem::CpuacctSubsystem(
    const Flags& _flags,
    const string& _hierarchy)
  : ProcessBase(process::ID::generate("cgroups-cpuacct-subsystem")),
    Subsystem(_flags, _hierarchy) {}


Future<ResourceStatistics> CpuacctSubsystem::usage(
    const ContainerID& containerId)
{
  ResourceStatistics result;

  // TODO(chzhcn): Getting the number of processes and threads is available as
  // long as any cgroup subsystem is used so this best not be tied to a specific
  // cgroup subsystem. A better place is probably Linux Launcher, which uses the
  // cgroup freezer subsystem. That requires some change for it to adopt the new
  // semantics of reporting subsystem-independent cgroup usage.
  // Note: The complexity of this operation is linear to the number of processes
  // and threads in a container: the kernel has to allocate memory to contain
  // the list of pids or tids; the userspace has to parse the cgroup files to
  // get the size. If this proves to be a performance bottleneck, some kind of
  // rate limiting mechanism needs to be employed.
  if (flags.cgroups_cpu_enable_pids_and_tids_count) {
    Try<set<pid_t>> pids = cgroups::processes(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()));

    if (pids.isError()) {
      return Failure("Failed to get number of processes: " + pids.error());
    }

    result.set_processes(pids.get().size());

    Try<set<pid_t>> tids = cgroups::threads(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()));

    if (tids.isError()) {
      return Failure("Failed to get number of threads: " + tids.error());
    }

    result.set_threads(tids.get().size());
  }

  // Get the number of clock ticks, used for cpu accounting.
  static long ticks = sysconf(_SC_CLK_TCK);

  PCHECK(ticks > 0) << "Failed to get sysconf(_SC_CLK_TCK)";

  // Add the cpuacct.stat information.
  Try<hashmap<string, uint64_t>> stat = cgroups::stat(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()),
      "cpuacct.stat");

  if (stat.isError()) {
    return Failure("Failed to read 'cpuacct.stat': " + stat.error());
  }

  // TODO(bmahler): Add namespacing to cgroups to enforce the expected
  // structure, e.g., cgroups::cpuacct::stat.
  Option<uint64_t> user = stat.get().get("user");
  Option<uint64_t> system = stat.get().get("system");

  if (user.isSome() && system.isSome()) {
    result.set_cpus_user_time_secs((double) user.get() / (double) ticks);
    result.set_cpus_system_time_secs((double) system.get() / (double) ticks);
  }

  return result;
}


MemorySubsystem::MemorySubsystem(
    const Flags& _flags,
    const string& _hierarchy)
  : ProcessBase(process::ID::generate("cgroups-memory-subsystem")),
    Subsystem(_flags, _hierarchy) {}


Try<Nothing> MemorySubsystem::load()
{
  // Make sure the kernel OOM-killer is enabled.
  // The Mesos OOM handler, as implemented, is not capable of handling the oom
  // condition by itself safely given the limitations Linux imposes on this code
  // path.
  Try<Nothing> enable = cgroups::memory::oom::killer::enable(
      hierarchy, flags.cgroups_root);

  if (enable.isError()) {
    return Error(enable.error());
  }

  // Test if memory pressure listening is enabled. We test that on the root
  // cgroup. We rely on 'Counter::create' to test if memory pressure listening
  // is enabled or not. The created counters will be destroyed immediately.
  foreach (const Level& level, levels()) {
    Try<Owned<Counter>> counter = Counter::create(
        hierarchy,
        flags.cgroups_root,
        level);

    if (counter.isError()) {
      return Error(
          "Failed to listen on '" + stringify(level) + "' memory events: " +
          counter.error());
    }
  }

  // Determine whether to limit swap or not.
  if (flags.cgroups_limit_swap) {
    Result<Bytes> check = cgroups::memory::memsw_limit_in_bytes(
        hierarchy, flags.cgroups_root);

    if (check.isError()) {
      return Error(
          "Failed to read 'memory.memsw.limit_in_bytes': " + check.error());
    } else if (check.isNone()) {
      return Error("'memory.memsw.limit_in_bytes' is not available");
    }
  }

  return Nothing();
}


Future<Nothing> MemorySubsystem::recover(const ContainerID& containerId)
{
  if (infos.contains(containerId)) {
    return Failure(
        "The subsystem '" + name() + "' of container " +
        stringify(containerId) + " has already been recovered");
  }

  infos.put(containerId, Owned<Info>(new Info));

  oomListen(containerId);
  pressureListen(containerId);

  return Nothing();
}


Future<Nothing> MemorySubsystem::prepare(const ContainerID& containerId)
{
  if (infos.contains(containerId)) {
    return Failure(
        "The subsystem '" + name() + "' of container " +
        stringify(containerId) + " has already been prepared");
  }

  infos.put(containerId, Owned<Info>(new Info));

  oomListen(containerId);
  pressureListen(containerId);

  return Nothing();
}


Future<Nothing> MemorySubsystem::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  if (!infos.contains(containerId)) {
    return Failure(
        "Failed to update subsystem '" + name() + "': Unknown container");
  }

  if (resources.mem().isNone()) {
    return Failure(
        "Failed to update subsystem '" + name() + "': No memory resource "
        "given");
  }

  // New limit.
  Bytes mem = resources.mem().get();
  Bytes limit = std::max(mem, MIN_MEMORY);

  // Always set the soft limit.
  Try<Nothing> write = cgroups::memory::soft_limit_in_bytes(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()),
      limit);

  if (write.isError()) {
    return Failure(
        "Failed to set 'memory.soft_limit_in_bytes': " + write.error());
  }

  LOG(INFO) << "Updated 'memory.soft_limit_in_bytes' to " << limit
            << " for container '" << containerId << "'";

  // Read the existing limit.
  Try<Bytes> currentLimit = cgroups::memory::limit_in_bytes(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()));

  // NOTE: If `cgroups_limit_swap` is (has been) used then both limit_in_bytes
  // and memsw.limit_in_bytes will always be set to the same value.
  if (currentLimit.isError()) {
    return Failure(
        "Failed to read 'memory.limit_in_bytes': " + currentLimit.error());
  }

  // Determine whether to set the hard limit. If this is the first time
  // (updatedLimit is false), or we're raising the existing limit, then we can
  // update the hard limit safely. Otherwise, if we need to decrease
  // 'memory.limit_in_bytes' we may induce an OOM if too much memory is in use.
  // As a result, we only update the soft limit when the memory reservation is
  // being reduced. This is probably okay if the machine has available
  // resources.
  // TODO(benh): Introduce a MemoryWatcherProcess which monitors the discrepancy
  // between usage and soft limit and introduces a "manual oom" if necessary.
  if (!infos[containerId]->updatedLimit || limit > currentLimit.get()) {
    // We always set limit_in_bytes first and optionally set
    // memsw.limit_in_bytes if `cgroups_limit_swap` is true.
    Try<Nothing> write = cgroups::memory::limit_in_bytes(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        limit);

    if (write.isError()) {
      return Failure(
          "Failed to set 'memory.limit_in_bytes': " + write.error());
    }

    LOG(INFO) << "Updated 'memory.limit_in_bytes' to " << limit
              << " for container '" << containerId << "'";

    if (flags.cgroups_limit_swap) {
      Try<bool> write = cgroups::memory::memsw_limit_in_bytes(
          hierarchy,
          path::join(flags.cgroups_root, containerId.value()),
          limit);

      if (write.isError()) {
        return Failure(
            "Failed to set 'memory.memsw.limit_in_bytes': " + write.error());
      }

      LOG(INFO) << "Updated 'memory.memsw.limit_in_bytes' to " << limit
                << " for container '" << containerId << "'";
    }

    infos[containerId]->updatedLimit = true;
  }

  return Nothing();
}


Future<ResourceStatistics> MemorySubsystem::usage(
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure(
        "Failed to usage subsystem '" + name() + "': Unknown container");
  }

  const Owned<Info>& info = infos[containerId];

  ResourceStatistics result;

  // The rss from memory.stat is wrong in two dimensions:
  //   1. It does not include child cgroups.
  //   2. It does not include any file backed pages.
  Try<Bytes> usage = cgroups::memory::usage_in_bytes(
      hierarchy, path::join(flags.cgroups_root, containerId.value()));

  if (usage.isError()) {
    return Failure("Failed to parse 'memory.usage_in_bytes': " + usage.error());
  }

  result.set_mem_total_bytes(usage.get().bytes());

  if (flags.cgroups_limit_swap) {
    Try<Bytes> usage = cgroups::memory::memsw_usage_in_bytes(
        hierarchy, path::join(flags.cgroups_root, containerId.value()));

    if (usage.isError()) {
      return Failure(
        "Failed to parse 'memory.memsw.usage_in_bytes': " + usage.error());
    }

    result.set_mem_total_memsw_bytes(usage.get().bytes());
  }

  // TODO(bmahler): Add namespacing to cgroups to enforce the expected
  // structure, e.g, cgroups::memory::stat.
  Try<hashmap<string, uint64_t>> stat = cgroups::stat(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()),
      "memory.stat");

  if (stat.isError()) {
    return Failure("Failed to read 'memory.stat': " + stat.error());
  }

  Option<uint64_t> total_cache = stat.get().get("total_cache");
  if (total_cache.isSome()) {
    // TODO(chzhcn): mem_file_bytes is deprecated in 0.23.0 and will
    // be removed in 0.24.0.
    result.set_mem_file_bytes(total_cache.get());

    result.set_mem_cache_bytes(total_cache.get());
  }

  Option<uint64_t> total_rss = stat.get().get("total_rss");
  if (total_rss.isSome()) {
    // TODO(chzhcn): mem_anon_bytes is deprecated in 0.23.0 and will
    // be removed in 0.24.0.
    result.set_mem_anon_bytes(total_rss.get());

    result.set_mem_rss_bytes(total_rss.get());
  }

  Option<uint64_t> total_mapped_file = stat.get().get("total_mapped_file");
  if (total_mapped_file.isSome()) {
    result.set_mem_mapped_file_bytes(total_mapped_file.get());
  }

  Option<uint64_t> total_swap = stat.get().get("total_swap");
  if (total_swap.isSome()) {
    result.set_mem_swap_bytes(total_swap.get());
  }

  Option<uint64_t> total_unevictable = stat.get().get("total_unevictable");
  if (total_unevictable.isSome()) {
    result.set_mem_unevictable_bytes(total_unevictable.get());
  }

  // Get pressure counter readings.
  list<Level> levels;
  list<Future<uint64_t>> values;
  foreachpair (Level level,
               const Owned<Counter>& counter,
               info->pressureCounters) {
    levels.push_back(level);
    values.push_back(counter->value());
  }

  return await(values)
    .then(defer(PID<MemorySubsystem>(this),
                &MemorySubsystem::_usage,
                containerId,
                result,
                levels,
                lambda::_1));
}


Future<ResourceStatistics> MemorySubsystem::_usage(
    const ContainerID& containerId,
    ResourceStatistics result,
    const list<Level>& levels,
    const list<Future<uint64_t>>& values)
{
  if (!infos.contains(containerId)) {
    return Failure(
        "Failed to _usage subsystem '" + name() + "': Unknown container");
  }

  list<Level>::const_iterator iterator = levels.begin();
  foreach (const Future<uint64_t>& value, values) {
    if (value.isReady()) {
      switch (*iterator) {
        case Level::LOW:
          result.set_mem_low_pressure_counter(value.get());
          break;
        case Level::MEDIUM:
          result.set_mem_medium_pressure_counter(value.get());
          break;
        case Level::CRITICAL:
          result.set_mem_critical_pressure_counter(value.get());
          break;
      }
    } else {
      LOG(ERROR) << "Failed to listen on '" << stringify(*iterator)
                 << "' pressure events for container " << containerId << ": "
                 << (value.isFailed() ? value.failure() : "discarded");
    }

    ++iterator;
  }

  return result;
}


Future<Nothing> MemorySubsystem::cleanup(const ContainerID& containerId)
{
  // Multiple calls may occur during test clean up.
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring cleanup subsystem '" << name()
            << "' request for unknown container: " << containerId;
    return Nothing();
  }

  if (infos[containerId]->oomNotifier.isPending()) {
    infos[containerId]->oomNotifier.discard();
  }

  infos.erase(containerId);

  return Nothing();
}


void MemorySubsystem::oomListen(const ContainerID& containerId)
{
  CHECK(infos.contains(containerId));

  infos[containerId]->oomNotifier = cgroups::memory::oom::listen(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()));

  // If the listening fails immediately, something very wrong happened.
  // Therefore, we report a fatal error here.
  if (infos[containerId]->oomNotifier.isFailed()) {
    LOG(FATAL) << "Failed to listen for OOM events for container "
               << containerId << ": "
               << infos[containerId]->oomNotifier.failure();
  }

  LOG(INFO) << "Started listening for OOM events for container "
            << containerId;

  infos[containerId]->oomNotifier.onReady(defer(
      PID<MemorySubsystem>(this),
      &MemorySubsystem::oomWaited,
      lambda::_1,
      containerId));
}


void MemorySubsystem::oomWaited(
    const Future<Nothing>& future,
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    LOG(ERROR) << "Received OOM notifier for unknown container " << containerId;
    return;
  }

  if (future.isDiscarded()) {
    LOG(INFO) << "Discarded OOM notifier for container " << containerId;
  } else if (future.isFailed()) {
    LOG(ERROR) << "Listening on OOM events failed for container "
               << containerId << ": " << future.failure();
  } else {
    // Out-of-memory event happened, call the handler.
    LOG(INFO) << "OOM notifier is triggered for container " << containerId;
    oom(containerId);
  }
}


void MemorySubsystem::oom(const ContainerID& containerId)
{
  CHECK(infos.contains(containerId));

  LOG(INFO) << "OOM detected for container " << containerId;

  // Construct a "message" string to describe why the isolator destroyed the
  // executor's cgroup (in order to assist in debugging).
  ostringstream message;
  message << "Memory limit exceeded: ";

  // Output the requested memory limit.
  // NOTE: If `cgroups_limit_swap` is (has been) used then both limit_in_bytes
  // and memsw.limit_in_bytes will always be set to the same value.
  Try<Bytes> limit = cgroups::memory::limit_in_bytes(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()));

  if (limit.isError()) {
    LOG(ERROR) << "Failed to read 'memory.limit_in_bytes': " << limit.error();
  } else {
    message << "Requested: " << limit.get() << " ";
  }

  // Output the maximum memory usage.
  Try<Bytes> usage = cgroups::memory::max_usage_in_bytes(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()));

  if (usage.isError()) {
    LOG(ERROR) << "Failed to read 'memory.max_usage_in_bytes': "
               << usage.error();
  } else {
    message << "Maximum Used: " << usage.get() << "\n";
  }

  // Output 'memory.stat' of the cgroup to help with debugging.
  // NOTE: With Kernel OOM-killer enabled these stats may not reflect memory
  // state at time of OOM.
  Try<string> read = cgroups::read(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()),
      "memory.stat");

  if (read.isError()) {
    LOG(ERROR) << "Failed to read 'memory.stat': " << read.error();
  } else {
    message << "\nMEMORY STATISTICS: \n" << read.get() << "\n";
  }

  LOG(INFO) << strings::trim(message.str()); // Trim the extra '\n' at the end.

  // TODO(jieyu): This is not accurate if the memory resource is from
  // a non-star role or spans roles (e.g., "*" and "role"). Ideally,
  // we should save the resources passed in and report it here.
  Resources mem = Resources::parse(
      "mem",
      stringify(usage.isSome() ? usage.get().megabytes() : 0),
      "*").get();

  ContainerLimitation limitation = protobuf::slave::createContainerLimitation(
      mem,
      message.str(),
      TaskStatus::REASON_CONTAINER_LIMITATION_MEMORY);

  notifyCallback(containerId, limitation);
}


void MemorySubsystem::pressureListen(const ContainerID& containerId)
{
  CHECK(infos.contains(containerId));

  foreach (const Level& level, levels()) {
    Try<Owned<Counter>> counter = Counter::create(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        level);

    if (counter.isError()) {
      LOG(ERROR) << "Failed to listen on '" << level << "' memory pressure "
                 << "events for container " << containerId << ": "
                 << counter.error();
    } else {
      infos[containerId]->pressureCounters[level] = counter.get();
      LOG(INFO) << "Started listening on '" << level << "' memory pressure "
                << "events for container " << containerId;
    }
  }
}


NetClsSubsystem::NetClsSubsystem(
    const Flags& _flags,
    const string& _hierarchy)
  : ProcessBase(process::ID::generate("cgroups-net-cls-subsystem")),
    Subsystem(_flags, _hierarchy) {}


Try<Nothing> NetClsSubsystem::load()
{
  IntervalSet<uint32_t> primaries;
  IntervalSet<uint32_t> secondaries;

  // Primary handle.
  if (flags.cgroups_net_cls_primary_handle.isSome()) {
    Try<uint16_t> primary = numify<uint16_t>(
        flags.cgroups_net_cls_primary_handle.get());

    if (primary.isError()) {
      return Error(
          "Failed to parse the primary handle '" +
          flags.cgroups_net_cls_primary_handle.get() +
          "' set in flag --cgroups_net_cls_primary_handle");
    }

    primaries +=
      (Bound<uint32_t>::closed(primary.get()),
       Bound<uint32_t>::closed(primary.get()));

    // Range of valid secondary handles.
    if (flags.cgroups_net_cls_secondary_handles.isSome()) {
      vector<string> range =
        strings::tokenize(flags.cgroups_net_cls_secondary_handles.get(), ",");

      if (range.size() != 2) {
        return Error(
            "Failed to parse the range of secondary handles '" +
            flags.cgroups_net_cls_secondary_handles.get() +
            "' set in flag --cgroups_net_cls_secondary_handles");
      }

      Try<uint16_t> lower = numify<uint16_t>(range[0]);
      if (lower.isError()) {
        return Error(
            "Failed to parse the lower bound of range of secondary handles '" +
            flags.cgroups_net_cls_secondary_handles.get() +
            "' set in flag --cgroups_net_cls_secondary_handles");
      }

      if (lower.get() == 0) {
        return Error("The secondary handle has to be a non-zero value.");
      }

      Try<uint16_t> upper =  numify<uint16_t>(range[1]);
      if (upper.isError()) {
        return Error(
            "Failed to parse the upper bound of range of secondary handles '" +
            flags.cgroups_net_cls_secondary_handles.get() +
            "' set in flag --cgroups_net_cls_secondary_handles");
      }

      secondaries +=
        (Bound<uint32_t>::closed(lower.get()),
         Bound<uint32_t>::closed(upper.get()));

      if (secondaries.empty()) {
        return Error(
            "Secondary handle range specified '" +
            flags.cgroups_net_cls_secondary_handles.get() +
            "', in flag --cgroups_net_cls_secondary_handles, is an empty set");
      }
    }
  }

  if (!primaries.empty()) {
    handleManager = NetClsHandleManager(primaries, secondaries);
  }

  return Nothing();
}


Future<Nothing> NetClsSubsystem::recover(const ContainerID& containerId)
{
  if (infos.contains(containerId)) {
    return Failure(
        "The subsystem '" + name() + "' of container " +
        stringify(containerId) + " has already been recovered");
  }

  // Read the net_cls handle.
  Result<NetClsHandle> handle = recoverHandle(
      hierarchy,
      path::join(flags.cgroups_root, containerId.value()));

  if (handle.isError()) {
    return Failure(
        "Failed to recover the net_cls handle for container " +
        stringify(containerId) + ": " + handle.error());
  }

  if (handle.isSome()) {
    infos.put(containerId, Owned<Info>(new Info(handle.get())));
  } else {
    infos.put(containerId, Owned<Info>(new Info));
  }

  return Nothing();
}


Future<Nothing> NetClsSubsystem::prepare(const ContainerID& containerId)
{
  if (infos.contains(containerId)) {
    return Failure(
        "The subsystem '" + name() + "' of container " +
        stringify(containerId) + " has already been prepared");
  }

  if (handleManager.isSome()) {
    Try<NetClsHandle> handle = handleManager->alloc();
    if (handle.isError()) {
      return Failure(
          "Failed to allocate a net_cls handle: " + handle.error());
    }

    LOG(INFO) << "Allocated a net_cls handle: " << handle.get()
              << " to container " << containerId;

    infos.put(containerId, Owned<Info>(new Info(handle.get())));
  } else {
    infos.put(containerId, Owned<Info>(new Info));
  }

  return Nothing();
}


Future<Nothing> NetClsSubsystem::isolate(
    const ContainerID& containerId, pid_t pid)
{
  if (!infos.contains(containerId)) {
    return Failure(
        "Failed to isolate subsystem '" + name() + "': Unknown container");
  }

  // If handle is not specified, the assumption is that the operator is
  // responsible for assigning the net_cls handles.
  if (infos[containerId]->handle.isSome()) {
    Try<Nothing> write = cgroups::net_cls::classid(
        hierarchy,
        path::join(flags.cgroups_root, containerId.value()),
        infos[containerId]->handle->get());

    if (write.isError()) {
      return Failure(
          "Failed to assign a net_cls handle to the cgroup: " + write.error());
    }
  }

  return Nothing();
}


Future<ContainerStatus> NetClsSubsystem::status(const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure(
        "Failed to status subsystem '" + name() + "': Unknown container");
  }

  ContainerStatus result;

  if (infos[containerId]->handle.isSome()) {
    VLOG(1) << "Updating container status with net_cls classid: "
            << infos[containerId]->handle.get();

    CgroupInfo* cgroupInfo = result.mutable_cgroup_info();
    CgroupInfo::NetCls* netCls = cgroupInfo->mutable_net_cls();

    netCls->set_classid(infos[containerId]->handle->get());
  }

  return result;
}


Future<Nothing> NetClsSubsystem::cleanup(const ContainerID& containerId)
{
  // Multiple calls may occur during test clean up.
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring cleanup subsystem '" << name()
            << "' request for unknown container: " << containerId;
    return Nothing();
  }

  if (infos[containerId]->handle.isSome() && handleManager.isSome()) {
    Try<Nothing> free = handleManager->free(infos[containerId]->handle.get());
    if (free.isError()) {
      LOG(ERROR) << "Failed to free the net_cls handle when cleanup subsystem '"
                 << name() << "': " << free.error();
      return Failure("Could not free the net_cls handle: " + free.error());
    }
  }

  infos.erase(containerId);

  return Nothing();
}


Result<NetClsHandle> NetClsSubsystem::recoverHandle(
    const std::string& hierarchy,
    const std::string& cgroup)
{
  Try<uint32_t> classid = cgroups::net_cls::classid(hierarchy, cgroup);
  if (classid.isError()) {
    return Error("Failed to read 'net_cls.classid': " + classid.error());
  }

  if (classid.get() == 0) {
    return None();
  }

  NetClsHandle handle(classid.get());

  // Mark the handle as used in handle manager.
  if (handleManager.isSome()) {
    Try<Nothing> reserve = handleManager->reserve(handle);
    if (reserve.isError()) {
      return Error("Failed to reserve the handle: " + reserve.error());
    }
  }

  return handle;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
