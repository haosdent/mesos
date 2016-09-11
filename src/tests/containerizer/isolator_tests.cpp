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

#include <unistd.h>

#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <mesos/resources.hpp>

#include <mesos/module/isolator.hpp>

#include <mesos/slave/isolator.hpp>

#include <process/future.hpp>
#include <process/io.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>

#include <stout/abort.hpp>
#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#ifdef __linux__
#include "linux/ns.hpp"
#endif // __linux__

#include "master/master.hpp"

#include "slave/flags.hpp"
#include "slave/gc.hpp"
#include "slave/slave.hpp"

#ifdef __linux__
#include "slave/containerizer/mesos/isolators/cgroups/net_cls.hpp"
#include "slave/containerizer/mesos/isolators/cgroups/perf_event.hpp"
#include "slave/containerizer/mesos/isolators/filesystem/shared.hpp"
#endif // __linux__
#include "slave/containerizer/mesos/isolators/posix.hpp"

#include "slave/containerizer/mesos/launcher.hpp"
#ifdef __linux__
#include "slave/containerizer/fetcher.hpp"
#include "slave/containerizer/mesos/containerizer.hpp"
#include "slave/containerizer/mesos/launch.hpp"
#include "slave/containerizer/mesos/linux_launcher.hpp"
#endif // __linux__

#include "tests/flags.hpp"
#include "tests/mesos.hpp"
#include "tests/module.hpp"
#include "tests/utils.hpp"

using namespace process;

using mesos::internal::master::Master;
#ifdef __linux__
using mesos::internal::slave::CgroupsNetClsIsolatorProcess;
using mesos::internal::slave::CgroupsPerfEventIsolatorProcess;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::LinuxLauncher;
using mesos::internal::slave::NetClsHandle;
using mesos::internal::slave::NetClsHandleManager;
using mesos::internal::slave::SharedFilesystemIsolatorProcess;
#endif // __linux__
using mesos::internal::slave::Launcher;
using mesos::internal::slave::MesosContainerizer;
using mesos::internal::slave::PosixLauncher;
using mesos::internal::slave::Slave;

using mesos::master::detector::MasterDetector;

using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::ContainerTermination;
using mesos::slave::Isolator;

using process::http::OK;
using process::http::Response;

using std::ostringstream;
using std::set;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace tests {

#ifdef __linux__
class NetClsHandleManagerTest : public testing::Test {};


// Tests the ability of the `NetClsHandleManager` class to allocate
// and free secondary handles from a range of primary handles.
TEST_F(NetClsHandleManagerTest, AllocateFreeHandles)
{
  NetClsHandleManager manager(IntervalSet<uint32_t>(
      (Bound<uint32_t>::closed(0x0002),
       Bound<uint32_t>::closed(0x0003))));

  Try<NetClsHandle> handle = manager.alloc(0x0003);
  ASSERT_SOME(handle);

  EXPECT_SOME_TRUE(manager.isUsed(handle.get()));

  ASSERT_SOME(manager.free(handle.get()));

  EXPECT_SOME_FALSE(manager.isUsed(handle.get()));
}


// Make sure allocation of secondary handles for invalid primary
// handles results in an error.
TEST_F(NetClsHandleManagerTest, AllocateInvalidPrimary)
{
  NetClsHandleManager manager(IntervalSet<uint32_t>(
      (Bound<uint32_t>::closed(0x0002),
       Bound<uint32_t>::closed(0x0003))));

  ASSERT_ERROR(manager.alloc(0x0001));
}


// Tests that we can reserve secondary handles for a given primary
// handle so that they won't be allocated out later.
TEST_F(NetClsHandleManagerTest, ReserveHandles)
{
  NetClsHandleManager manager(IntervalSet<uint32_t>(
      (Bound<uint32_t>::closed(0x0002),
       Bound<uint32_t>::closed(0x0003))));

  NetClsHandle handle(0x0003, 0xffff);

  ASSERT_SOME(manager.reserve(handle));

  EXPECT_SOME_TRUE(manager.isUsed(handle));
}


// Tests that secondary handles are allocated only from a given range,
// when the range is specified.
TEST_F(NetClsHandleManagerTest, SecondaryHandleRange)
{
  NetClsHandleManager manager(
      IntervalSet<uint32_t>(
        (Bound<uint32_t>::closed(0x0002),
         Bound<uint32_t>::closed(0x0003))),
      IntervalSet<uint32_t>(
        (Bound<uint32_t>::closed(0xffff),
         Bound<uint32_t>::closed(0xffff))));

  Try<NetClsHandle> handle = manager.alloc(0x0003);
  ASSERT_SOME(handle);

  EXPECT_SOME_TRUE(manager.isUsed(handle.get()));

  // Try allocating another handle. This should fail, since we don't
  // have any more secondary handles left.
  EXPECT_ERROR(manager.alloc(0x0003));

  ASSERT_SOME(manager.free(handle.get()));

  ASSERT_SOME(manager.reserve(handle.get()));

  EXPECT_SOME_TRUE(manager.isUsed(handle.get()));

  // Make sure you cannot reserve a secondary handle that is out of
  // range.
  EXPECT_ERROR(manager.reserve(NetClsHandle(0x0003, 0x0001)));
}
#endif // __linux__


#ifdef __linux__
class NetClsIsolatorTest : public MesosTest {};


// This tests the create, prepare, isolate and cleanup methods of the
// 'CgroupNetClsIsolatorProcess'. The test first creates a 'MesosContainerizer'
// with net_cls cgroup isolator enabled. The net_cls cgroup isolator is
// implemented in the 'CgroupNetClsIsolatorProcess' class. The test then
// launches a task in a mesos container and checks to see if the container has
// been added to the right net_cls cgroup. Finally, the test kills the task and
// makes sure that the 'CgroupNetClsIsolatorProcess' cleans up the net_cls
// cgroup created for the container.
TEST_F(NetClsIsolatorTest, ROOT_CGROUPS_NET_CLS_Isolate)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  uint16_t primary = 0x0012;

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "cgroups/net_cls";
  flags.cgroups_net_cls_primary_handle = stringify(primary);
  flags.cgroups_net_cls_secondary_handles = "0xffff,0xffff";

  Fetcher fetcher;

  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  ASSERT_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;

  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> schedRegistered;
  EXPECT_CALL(sched, registered(_, _, _))
    .WillOnce(FutureSatisfy(&schedRegistered));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());      // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(schedRegistered);

  AWAIT_READY(offers);
  EXPECT_EQ(1u, offers.get().size());

  // Create a task to be launched in the mesos-container. We will be
  // explicitly killing this task to perform the cleanup test.
  TaskInfo task = createTask(offers.get()[0], "sleep 1000");

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  // Capture the update to verify that the task has been launched.
  AWAIT_READY(status);
  ASSERT_EQ(TASK_RUNNING, status.get().state());

  // Task is ready.  Make sure there is exactly 1 container in the hashset.
  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  EXPECT_EQ(1u, containers.get().size());

  const ContainerID& containerID = *(containers.get().begin());

  Result<string> hierarchy = cgroups::hierarchy("net_cls");
  ASSERT_SOME(hierarchy);

  // Check if the net_cls cgroup for this container exists, by
  // checking for the processes associated with this cgroup.
  string cgroup = path::join(
      flags.cgroups_root,
      containerID.value());

  Try<set<pid_t>> pids = cgroups::processes(hierarchy.get(), cgroup);
  ASSERT_SOME(pids);

  // There should be at least one TGID associated with this cgroup.
  EXPECT_LE(1u, pids.get().size());

  // Read the `net_cls.classid` to verify that the handle has been set.
  Try<uint32_t> classid = cgroups::net_cls::classid(hierarchy.get(), cgroup);
  EXPECT_SOME(classid);

  if (classid.isSome()) {
    // Make sure the primary handle is the same as the one set in
    // `--cgroup_net_cls_primary_handle`.
    EXPECT_EQ(primary, (classid.get() & 0xffff0000) >> 16);

    // Make sure the secondary handle is 0xffff.
    EXPECT_EQ(0xffffu, classid.get() & 0xffff);
  }

  // Isolator cleanup test: Killing the task should cleanup the cgroup
  // associated with the container.
  Future<TaskStatus> killStatus;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&killStatus));

  // Wait for the executor to exit. We are using 'gc.schedule' as a proxy event
  // to monitor the exit of the executor.
  Future<Nothing> gcSchedule = FUTURE_DISPATCH(
      _, &slave::GarbageCollectorProcess::schedule);

  driver.killTask(status.get().task_id());

  AWAIT_READY(gcSchedule);

  // If the cleanup is successful the net_cls cgroup for this container should
  // not exist.
  ASSERT_FALSE(os::exists(cgroup));

  driver.stop();
  driver.join();
}


// This test verifies that we are able to retrieve the `net_cls` handle
// from `/state`.
TEST_F(NetClsIsolatorTest, ROOT_CGROUPS_NET_CLS_ContainerStatus)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "cgroups/net_cls";
  flags.cgroups_net_cls_primary_handle = stringify(0x0012);
  flags.cgroups_net_cls_secondary_handles = "0x0011,0x0012";

  Fetcher fetcher;

  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  ASSERT_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;

  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> schedRegistered;
  EXPECT_CALL(sched, registered(_, _, _))
    .WillOnce(FutureSatisfy(&schedRegistered));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());      // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(schedRegistered);

  AWAIT_READY(offers);
  EXPECT_EQ(1u, offers.get().size());

  // Create a task to be launched in the mesos-container.
  TaskInfo task = createTask(offers.get()[0], "sleep 1000");

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  ASSERT_EQ(TASK_RUNNING, status.get().state());

  // Task is ready. Verify `ContainerStatus` is present in slave state.
  Future<Response> response = http::get(
      slave.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ(APPLICATION_JSON, "Content-Type", response);

  Try<JSON::Object> parse = JSON::parse<JSON::Object>(response.get().body);
  ASSERT_SOME(parse);

  Result<JSON::Object> netCls = parse->find<JSON::Object>(
      "frameworks[0].executors[0].tasks[0].statuses[0]."
      "container_status.cgroup_info.net_cls");
  ASSERT_SOME(netCls);

  uint32_t classid =
    netCls->values["classid"].as<JSON::Number>().as<uint32_t>();

  // Check the primary and the secondary handle.
  EXPECT_EQ(0x0012u, classid >> 16);
  EXPECT_EQ(0x0011u, classid & 0xffff);

  driver.stop();
  driver.join();
}
#endif


#ifdef __linux__
class PerfEventIsolatorTest : public MesosTest {};


TEST_F(PerfEventIsolatorTest, ROOT_CGROUPS_PERF_Sample)
{
  slave::Flags flags;

  flags.perf_events = "cycles,task-clock";
  flags.perf_duration = Milliseconds(250);
  flags.perf_interval = Milliseconds(500);

  Try<Isolator*> _isolator = CgroupsPerfEventIsolatorProcess::create(flags);
  ASSERT_SOME(_isolator);
  Owned<Isolator> isolator(_isolator.get());

  ExecutorInfo executorInfo;

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  // Use a relative temporary directory so it gets cleaned up
  // automatically with the test.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  ContainerConfig containerConfig;
  containerConfig.mutable_executor_info()->CopyFrom(executorInfo);
  containerConfig.set_directory(dir.get());

  AWAIT_READY(isolator->prepare(
      containerId,
      containerConfig));

  // This first sample is likely to be empty because perf hasn't
  // completed yet but we should still have the required fields.
  Future<ResourceStatistics> statistics1 = isolator->usage(containerId);
  AWAIT_READY(statistics1);
  ASSERT_TRUE(statistics1.get().has_perf());
  EXPECT_TRUE(statistics1.get().perf().has_timestamp());
  EXPECT_TRUE(statistics1.get().perf().has_duration());

  // Wait until we get the next sample. We use a generous timeout of
  // two seconds because we currently have a one second reap interval;
  // when running perf with perf_duration of 250ms we won't notice the
  // exit for up to one second.
  ResourceStatistics statistics2;
  Duration waited = Duration::zero();
  do {
    Future<ResourceStatistics> statistics = isolator->usage(containerId);
    AWAIT_READY(statistics);

    statistics2 = statistics.get();

    ASSERT_TRUE(statistics2.has_perf());

    if (statistics1.get().perf().timestamp() !=
        statistics2.perf().timestamp()) {
      break;
    }

    os::sleep(Milliseconds(250));
    waited += Milliseconds(250);
  } while (waited < Seconds(2));

  sleep(2);

  EXPECT_NE(statistics1.get().perf().timestamp(),
            statistics2.perf().timestamp());

  EXPECT_TRUE(statistics2.perf().has_cycles());
  EXPECT_LE(0u, statistics2.perf().cycles());

  EXPECT_TRUE(statistics2.perf().has_task_clock());
  EXPECT_LE(0.0, statistics2.perf().task_clock());

  AWAIT_READY(isolator->cleanup(containerId));
}


class SharedFilesystemIsolatorTest : public MesosTest {};


// Test that a container can create a private view of a system
// directory (/var/tmp). Check that a file written by a process inside
// the container doesn't appear on the host filesystem but does appear
// under the container's work directory.
// This test is disabled since we're planning to remove the shared
// filesystem isolator and this test is not working on other distros
// such as CentOS 7.1
// TODO(tnachen): Remove this test when shared filesystem isolator
// is removed.
TEST_F(SharedFilesystemIsolatorTest, DISABLED_ROOT_RelativeVolume)
{
  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "filesystem/shared";

  Try<Isolator*> _isolator = SharedFilesystemIsolatorProcess::create(flags);
  ASSERT_SOME(_isolator);
  Owned<Isolator> isolator(_isolator.get());

  Try<Launcher*> _launcher = LinuxLauncher::create(flags);
  ASSERT_SOME(_launcher);
  Owned<Launcher> launcher(_launcher.get());

  // Use /var/tmp so we don't mask the work directory (under /tmp).
  const string containerPath = "/var/tmp";
  ASSERT_TRUE(os::stat::isdir(containerPath));

  // Use a host path relative to the container work directory.
  const string hostPath = strings::remove(containerPath, "/", strings::PREFIX);

  ContainerInfo containerInfo;
  containerInfo.set_type(ContainerInfo::MESOS);
  containerInfo.add_volumes()->CopyFrom(
      CREATE_VOLUME(containerPath, hostPath, Volume::RW));

  ExecutorInfo executorInfo;
  executorInfo.mutable_container()->CopyFrom(containerInfo);

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  ContainerConfig containerConfig;
  containerConfig.mutable_executor_info()->CopyFrom(executorInfo);
  containerConfig.set_directory(flags.work_dir);

  Future<Option<ContainerLaunchInfo>> prepare =
    isolator->prepare(
        containerId,
        containerConfig);

  AWAIT_READY(prepare);
  ASSERT_SOME(prepare.get());
  ASSERT_EQ(1, prepare.get().get().pre_exec_commands().size());
  EXPECT_TRUE(prepare.get().get().has_namespaces());

  // The test will touch a file in container path.
  const string file = path::join(containerPath, UUID::random().toString());
  ASSERT_FALSE(os::exists(file));

  // Manually run the isolator's preparation command first, then touch
  // the file.
  vector<string> args;
  args.push_back("sh");
  args.push_back("-x");
  args.push_back("-c");
  args.push_back(
      prepare.get().get().pre_exec_commands(0).value() + " && touch " + file);

  Try<pid_t> pid = launcher->fork(
      containerId,
      "sh",
      args,
      Subprocess::FD(STDIN_FILENO),
      Subprocess::FD(STDOUT_FILENO),
      Subprocess::FD(STDERR_FILENO),
      nullptr,
      None(),
      prepare.get().get().namespaces());
  ASSERT_SOME(pid);

  // Set up the reaper to wait on the forked child.
  Future<Option<int>> status = process::reap(pid.get());

  AWAIT_READY(status);
  EXPECT_SOME_EQ(0, status.get());

  // Check the correct hierarchy was created under the container work
  // directory.
  string dir = "/";
  foreach (const string& subdir, strings::tokenize(containerPath, "/")) {
    dir = path::join(dir, subdir);

    struct stat hostStat;
    EXPECT_EQ(0, ::stat(dir.c_str(), &hostStat));

    struct stat containerStat;
    EXPECT_EQ(0,
              ::stat(path::join(flags.work_dir, dir).c_str(), &containerStat));

    EXPECT_EQ(hostStat.st_mode, containerStat.st_mode);
    EXPECT_EQ(hostStat.st_uid, containerStat.st_uid);
    EXPECT_EQ(hostStat.st_gid, containerStat.st_gid);
  }

  // Check it did *not* create a file in the host namespace.
  EXPECT_FALSE(os::exists(file));

  // Check it did create the file under the container's work directory
  // on the host.
  EXPECT_TRUE(os::exists(path::join(flags.work_dir, file)));
}


// This test is disabled since we're planning to remove the shared
// filesystem isolator and this test is not working on other distros
// such as CentOS 7.1
// TODO(tnachen): Remove this test when shared filesystem isolator
// is removed.
TEST_F(SharedFilesystemIsolatorTest, DISABLED_ROOT_AbsoluteVolume)
{
  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "filesystem/shared";

  Try<Isolator*> _isolator = SharedFilesystemIsolatorProcess::create(flags);
  ASSERT_SOME(_isolator);
  Owned<Isolator> isolator(_isolator.get());

  Try<Launcher*> _launcher = LinuxLauncher::create(flags);
  ASSERT_SOME(_launcher);
  Owned<Launcher> launcher(_launcher.get());

  // We'll mount the absolute test work directory as /var/tmp in the
  // container.
  const string hostPath = flags.work_dir;
  const string containerPath = "/var/tmp";

  ContainerInfo containerInfo;
  containerInfo.set_type(ContainerInfo::MESOS);
  containerInfo.add_volumes()->CopyFrom(
      CREATE_VOLUME(containerPath, hostPath, Volume::RW));

  ExecutorInfo executorInfo;
  executorInfo.mutable_container()->CopyFrom(containerInfo);

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  ContainerConfig containerConfig;
  containerConfig.mutable_executor_info()->CopyFrom(executorInfo);
  containerConfig.set_directory(flags.work_dir);

  Future<Option<ContainerLaunchInfo>> prepare =
    isolator->prepare(
        containerId,
        containerConfig);

  AWAIT_READY(prepare);
  ASSERT_SOME(prepare.get());
  ASSERT_EQ(1, prepare.get().get().pre_exec_commands().size());
  EXPECT_TRUE(prepare.get().get().has_namespaces());

  // Test the volume mounting by touching a file in the container's
  // /tmp, which should then be in flags.work_dir.
  const string filename = UUID::random().toString();
  ASSERT_FALSE(os::exists(path::join(containerPath, filename)));

  vector<string> args;
  args.push_back("sh");
  args.push_back("-x");
  args.push_back("-c");
  args.push_back(prepare.get().get().pre_exec_commands(0).value() +
                 " && touch " +
                 path::join(containerPath, filename));

  Try<pid_t> pid = launcher->fork(
      containerId,
      "sh",
      args,
      Subprocess::FD(STDIN_FILENO),
      Subprocess::FD(STDOUT_FILENO),
      Subprocess::FD(STDERR_FILENO),
      nullptr,
      None(),
      prepare.get().get().namespaces());
  ASSERT_SOME(pid);

  // Set up the reaper to wait on the forked child.
  Future<Option<int>> status = process::reap(pid.get());

  AWAIT_READY(status);
  EXPECT_SOME_EQ(0, status.get());

  // Check the file was created in flags.work_dir.
  EXPECT_TRUE(os::exists(path::join(hostPath, filename)));

  // Check it didn't get created in the host's view of containerPath.
  EXPECT_FALSE(os::exists(path::join(containerPath, filename)));
}


class NamespacesPidIsolatorTest : public MesosTest {};


TEST_F(NamespacesPidIsolatorTest, ROOT_PidNamespace)
{
  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "namespaces/pid";

  string directory = os::getcwd(); // We're inside a temporary sandbox.

  Fetcher fetcher;

  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, false, &fetcher);

  ASSERT_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  // Write the command's pid namespace inode and init name to files.
  const string command =
    "stat -c %i /proc/self/ns/pid > ns && (cat /proc/1/comm > init)";

  process::Future<bool> launch = containerizer->launch(
      containerId,
      None(),
      CREATE_EXECUTOR_INFO("executor", command),
      directory,
      None(),
      SlaveID(),
      std::map<string, string>(),
      false);
  AWAIT_READY(launch);
  ASSERT_TRUE(launch.get());

  // Wait on the container.
  Future<ContainerTermination> wait = containerizer->wait(containerId);
  AWAIT_READY(wait);

  // Check the executor exited correctly.
  EXPECT_TRUE(wait.get().has_status());
  EXPECT_EQ(0, wait.get().status());

  // Check that the command was run in a different pid namespace.
  Try<ino_t> testPidNamespace = ns::getns(::getpid(), "pid");
  ASSERT_SOME(testPidNamespace);

  Try<string> containerPidNamespace = os::read(path::join(directory, "ns"));
  ASSERT_SOME(containerPidNamespace);

  EXPECT_NE(stringify(testPidNamespace.get()),
            strings::trim(containerPidNamespace.get()));

  // Check that 'sh' is the container's 'init' process.
  // This verifies that /proc has been correctly mounted for the container.
  Try<string> init = os::read(path::join(directory, "init"));
  ASSERT_SOME(init);

  EXPECT_EQ("sh", strings::trim(init.get()));
}

#endif // __linux__

} // namespace tests {
} // namespace internal {
} // namespace mesos {
