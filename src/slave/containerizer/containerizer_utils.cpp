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

#include <stout/bytes.hpp>
#include <stout/fs.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include "hook/manager.hpp"

#include "slave/constants.hpp"
#include "slave/flags.hpp"

#include "slave/containerizer/containerizer_utils.hpp"

using std::map;
using std::string;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

// TODO(idownes): Move this to the Containerizer interface to complete
// the delegation of containerization, i.e., external containerizers should be
// able to report the resources they can isolate.
Try<Resources> containerizerResources(const Flags& flags)
{
  Try<Resources> parsed = Resources::parse(
      flags.resources.getOrElse(""), flags.default_role);

  if (parsed.isError()) {
    return Error(parsed.error());
  }

  Resources resources = parsed.get();

  // NOTE: We need to check for the "cpus" string within the flag
  // because once Resources are parsed, we cannot distinguish between
  //  (1) "cpus:0", and
  //  (2) no cpus specified.
  // We only auto-detect cpus in case (2).
  // The same logic applies for the other resources!
  if (!strings::contains(flags.resources.getOrElse(""), "cpus")) {
    // No CPU specified so probe OS or resort to DEFAULT_CPUS.
    double cpus;
    Try<long> cpus_ = os::cpus();
    if (!cpus_.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the number of cpus to use: '"
                   << cpus_.error()
                   << "'; defaulting to " << DEFAULT_CPUS;
      cpus = DEFAULT_CPUS;
    } else {
      cpus = cpus_.get();
    }

    resources += Resources::parse(
        "cpus",
        stringify(cpus),
        flags.default_role).get();
  }

  // GPU resource.
  // We currently do not support GPU discovery, so we require that
  // GPUs are explicitly specified in `--resources`. When Nvidia GPU
  // support is enabled, we also require the GPU devices to be
  // specified in `--nvidia_gpu_devices`.
  if (strings::contains(flags.resources.getOrElse(""), "gpus")) {
    // Make sure that the value of `gpus` is actually an integer and
    // not a fractional amount. We take advantage of the fact that we
    // know the value of `gpus` is only precise up to 3 decimals.
    long long millis = static_cast<long long>(resources.gpus().get() * 1000);
    if ((millis % 1000) != 0) {
      return Error("The `gpus` resource must specified as an unsigned integer");
    }

#ifdef ENABLE_NVIDIA_GPU_SUPPORT
    // Verify that the number of GPUs in `--nvidia_gpu_devices`
    // matches the number of GPUs specified as a resource. In the
    // future we will do discovery of GPUs, which will make the
    // `--nvidia_gpu_devices` flag optional.
    if (!flags.nvidia_gpu_devices.isSome()) {
      return Error("When specifying the `gpus` resource, you must also specify"
                   " a list of GPUs via the `--nvidia_gpu_devices` flag");
    }

    if (flags.nvidia_gpu_devices->size() != resources.gpus().get())
      return Error("The number of GPUs passed in the '--nvidia_gpu_devices'"
                   " flag must match the number of GPUs specified in the 'gpus'"
                   " resource");
#endif // ENABLE_NVIDIA_GPU_SUPPORT
  }

  // Memory resource.
  if (!strings::contains(flags.resources.getOrElse(""), "mem")) {
    // No memory specified so probe OS or resort to DEFAULT_MEM.
    Bytes mem;
    Try<os::Memory> mem_ = os::memory();
    if (mem_.isError()) {
      LOG(WARNING) << "Failed to auto-detect the size of main memory: '"
                    << mem_.error()
                    << "' ; defaulting to DEFAULT_MEM";
      mem = DEFAULT_MEM;
    } else {
      Bytes total = mem_.get().total;
      if (total >= Gigabytes(2)) {
        mem = total - Gigabytes(1); // Leave 1GB free.
      } else {
        mem = Bytes(total.bytes() / 2); // Use 50% of the memory.
      }
    }

    resources += Resources::parse(
        "mem",
        stringify(mem.megabytes()),
        flags.default_role).get();
  }

  // Disk resource.
  if (!strings::contains(flags.resources.getOrElse(""), "disk")) {
    // No disk specified so probe OS or resort to DEFAULT_DISK.
    Bytes disk;

    // NOTE: We calculate disk size of the file system on
    // which the slave work directory is mounted.
    Try<Bytes> disk_ = fs::size(flags.work_dir);
    if (!disk_.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the disk space: '"
                   << disk_.error()
                   << "' ; defaulting to " << DEFAULT_DISK;
      disk = DEFAULT_DISK;
    } else {
      Bytes total = disk_.get();
      if (total >= Gigabytes(10)) {
        disk = total - Gigabytes(5); // Leave 5GB free.
      } else {
        disk = Bytes(total.bytes() / 2); // Use 50% of the disk.
      }
    }

    resources += Resources::parse(
        "disk",
        stringify(disk.megabytes()),
        flags.default_role).get();
  }

  // Network resource.
  if (!strings::contains(flags.resources.getOrElse(""), "ports")) {
    // No ports specified so resort to DEFAULT_PORTS.
    resources += Resources::parse(
        "ports",
        stringify(DEFAULT_PORTS),
        flags.default_role).get();
  }

  Option<Error> error = Resources::validate(resources);
  if (error.isSome()) {
    return error.get();
  }

  return resources;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
