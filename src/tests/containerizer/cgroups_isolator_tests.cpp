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

#include <process/gtest.hpp>

#include <stout/gtest.hpp>

#include <process/queue.hpp>

#include "slave/containerizer/mesos/containerizer.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/constants.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/subsystems/net_cls.hpp"

#include "tests/mesos.hpp"
#include "tests/script.hpp"

using mesos::internal::master::Master;

using mesos::internal::slave::CGROUP_SUBSYSTEM_CPU_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_CPUACCT_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_DEVICES_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_MEMORY_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_NET_CLS_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_PERF_EVENT_NAME;
using mesos::internal::slave::CPU_SHARES_PER_CPU_REVOCABLE;
using mesos::internal::slave::DEFAULT_EXECUTOR_CPUS;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;
using mesos::internal::slave::NetClsHandle;
using mesos::internal::slave::NetClsHandleManager;

using mesos::master::detector::MasterDetector;

using process::Future;
using process::Owned;
using process::Queue;

using process::http::OK;
using process::http::Response;

using std::set;
using std::string;
using std::vector;

using testing::InvokeWithoutArgs;

namespace mesos {
namespace internal {
namespace tests {


// Run the balloon framework under a mesos containerizer.
TEST_SCRIPT(ContainerizerTest,
            ROOT_CGROUPS_BalloonFramework,
            "balloon_framework_test.sh")


// Username for the unprivileged user that will be created to test
// unprivileged cgroup creation. It will be removed after the tests.
// It is presumed this user does not normally exist.
const string UNPRIVILEGED_USERNAME = "mesos.test.unprivileged.user";


class UserCgroupsIsolatorTest
  : public ContainerizerTest<slave::MesosContainerizer>
{
public:
  static void SetUpTestCase()
  {
    ContainerizerTest<slave::MesosContainerizer>::SetUpTestCase();

    // Remove the user in case it wasn't cleaned up from a previous
    // test.
    os::system("userdel -r " + UNPRIVILEGED_USERNAME + " > /dev/null");

    ASSERT_EQ(0, os::system("useradd " + UNPRIVILEGED_USERNAME));
  }


  static void TearDownTestCase()
  {
    ContainerizerTest<slave::MesosContainerizer>::TearDownTestCase();

    ASSERT_EQ(0, os::system("userdel -r " + UNPRIVILEGED_USERNAME));
  }
};


TEST_F(UserCgroupsIsolatorTest, ROOT_CGROUPS_PERF_NET_CLS_UserCgroup)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.perf_events = "cpu-cycles"; // Needed for `PerfEventSubsystem`.
  flags.isolation = "cgroups/cpu,cgroups/devices,cgroups/mem,cgroups/net_cls,"
                    "cgroups/perf_event";

  Fetcher fetcher;
  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  CHECK_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get());

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  // Launch a task with the command executor.
  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(offers.get()[0].resources());

  // Command executor will run as user running test.
  CommandInfo command;
  command.set_shell(true);
  command.set_value("sleep 120");
  command.set_user(UNPRIVILEGED_USERNAME);

  task.mutable_command()->MergeFrom(command);

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  set<string> subsystems = {
    CGROUP_SUBSYSTEM_CPU_NAME,
    CGROUP_SUBSYSTEM_CPUACCT_NAME,
    CGROUP_SUBSYSTEM_DEVICES_NAME,
    CGROUP_SUBSYSTEM_MEMORY_NAME,
    CGROUP_SUBSYSTEM_NET_CLS_NAME,
    CGROUP_SUBSYSTEM_PERF_EVENT_NAME,
  };

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  EXPECT_EQ(1u, containers.get().size());

  ContainerID containerId = *(containers.get().begin());

  foreach (string subsystem, subsystems) {
    string hierarchy = path::join(flags.cgroups_hierarchy, subsystem);
    string cgroup = path::join(flags.cgroups_root, containerId.value());

    // Check the user cannot manipulate the container's cgroup control
    // files.
    EXPECT_NE(0, os::system(
          "su - " + UNPRIVILEGED_USERNAME +
          " -c 'echo $$ >" +
          path::join(hierarchy, cgroup, "cgroup.procs") +
          "'"));

    // Check the user can create a cgroup under the container's
    // cgroup.
    string userCgroup = path::join(cgroup, "user");

    EXPECT_EQ(0, os::system(
          "su - " +
          UNPRIVILEGED_USERNAME +
          " -c 'mkdir " +
          path::join(hierarchy, userCgroup) +
          "'"));

    // Check the user can manipulate control files in the created
    // cgroup.
    EXPECT_EQ(0, os::system(
          "su - " +
          UNPRIVILEGED_USERNAME +
          " -c 'echo $$ >" +
          path::join(hierarchy, userCgroup, "cgroup.procs") +
          "'"));

    // Clear up the folder.
    AWAIT_READY(cgroups::destroy(hierarchy, userCgroup));
  }

  driver.stop();
  driver.join();
}


class CgroupsIsolatorTest : public MesosTest {};


TEST_F(CgroupsIsolatorTest, ROOT_CGROUPS_RevocableCpu)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "cgroups/cpu";

  Fetcher fetcher;
  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  CHECK_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  MockResourceEstimator resourceEstimator;

  EXPECT_CALL(resourceEstimator, initialize(_));

  Queue<Resources> estimations;
  EXPECT_CALL(resourceEstimator, oversubscribable())
    .WillRepeatedly(InvokeWithoutArgs(&estimations, &Queue<Resources>::get));

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get(), &resourceEstimator, flags);

  ASSERT_SOME(slave);

  // Start the framework which accepts revocable resources.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.add_capabilities()->set_type(
      FrameworkInfo::Capability::REVOCABLE_RESOURCES);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<vector<Offer>> offers1;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers1));

  driver.start();

  // Initially the framework will get all regular resources.
  AWAIT_READY(offers1);
  EXPECT_NE(0u, offers1.get().size());
  EXPECT_TRUE(Resources(offers1.get()[0].resources()).revocable().empty());

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  // Inject an estimation of revocable cpu resources.
  Resource cpu = Resources::parse("cpus", "1", "*").get();
  cpu.mutable_revocable();
  Resources cpus(cpu);
  estimations.put(cpus);

  // Now the framework will get revocable resources.
  AWAIT_READY(offers2);
  EXPECT_NE(0u, offers2.get().size());
  EXPECT_EQ(cpus, Resources(offers2.get()[0].resources()));

  CommandInfo command;
  command.set_shell(true);
  command.set_value("sleep 120");

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers2.get()[0].slave_id());
  task.mutable_command()->MergeFrom(command);
  task.mutable_resources()->MergeFrom(cpus);

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers2.get()[0].id(), {task});

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  EXPECT_EQ(1u, containers.get().size());

  ContainerID containerId = *(containers.get().begin());

  string cpuHierarchy = path::join(flags.cgroups_hierarchy, "cpu");
  string cpuCgroup= path::join(flags.cgroups_root, containerId.value());

  double totalCpus = cpus.cpus().get() + DEFAULT_EXECUTOR_CPUS;
  EXPECT_SOME_EQ(
      CPU_SHARES_PER_CPU_REVOCABLE * totalCpus,
      cgroups::cpu::shares(cpuHierarchy, cpuCgroup));

  driver.stop();
  driver.join();
}


TEST_F(CgroupsIsolatorTest, ROOT_CGROUPS_CFS_EnableCfs)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "cgroups/cpu";
  // Enable CFS to cap CPU utilization.
  flags.cgroups_enable_cfs = true;

  Fetcher fetcher;
  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  CHECK_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get());

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  // Launch a task with the command executor.
  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());

  ASSERT_GE(Resources(offers.get()[0].resources()).cpus().get(), 0.5);
  task.mutable_resources()->MergeFrom(Resources::parse("cpus:0.5").get());

  CommandInfo command;
  command.set_shell(true);
  // Generate random numbers to max out a single core. We'll run this for 0.5
  // seconds of wall time so it should consume approximately 250 ms of total
  // cpu time when limited to 0.5 cpu. We use /dev/urandom to prevent blocking
  // on Linux when there's insufficient entropy.
  command.set_value("cat /dev/urandom > /dev/null & "
    "export MESOS_TEST_PID=$! && "
    "sleep 0.5 && "
    "kill $MESOS_TEST_PID");

  task.mutable_command()->MergeFrom(command);

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  EXPECT_EQ(1u, containers.get().size());

  ContainerID containerId = *(containers.get().begin());

  Future<ResourceStatistics> usage = containerizer->usage(containerId);
  AWAIT_READY(usage);

  // Expect that no more than 300 ms of cpu time has been consumed. We also
  // check that at least 50 ms of cpu time has been consumed so this test will
  // fail if the host system is very heavily loaded. This behavior is correct
  // because under such conditions we aren't actually testing the CFS cpu
  // limiter.
  double cpuTime = usage.get().cpus_system_time_secs() +
                   usage.get().cpus_user_time_secs();

  EXPECT_GE(0.30, cpuTime);
  EXPECT_LE(0.05, cpuTime);

  driver.stop();
  driver.join();
}


// A test to verify the number of processes and threads in a
// container.
TEST_F(CgroupsIsolatorTest, ROOT_CGROUPS_PidsAndTids)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "cgroups/cpu";
  flags.cgroups_cpu_enable_pids_and_tids_count = true;

  Fetcher fetcher;
  Try<MesosContainerizer*> _containerizer =
    MesosContainerizer::create(flags, true, &fetcher);

  CHECK_SOME(_containerizer);
  Owned<MesosContainerizer> containerizer(_containerizer.get());

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get());

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  // Launch a task with the command executor.
  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(offers.get()[0].resources());

  CommandInfo command;
  command.set_shell(false);
  command.set_value("/bin/cat");
  command.add_arguments("/bin/cat");

  task.mutable_command()->MergeFrom(command);

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  EXPECT_EQ(1u, containers.get().size());

  ContainerID containerId = *(containers.get().begin());

  Future<ResourceStatistics> usage = containerizer->usage(containerId);
  AWAIT_READY(usage);

  // The possible running processes during capture process number.
  //   - src/.libs/mesos-executor
  //   - src/mesos-executor
  //   - src/.libs/mesos-containerizer
  //   - src/mesos-containerizer
  //   - cat
  // For `cat` and `mesos-executor`, they keep idling during running
  // the test case. For other processes, they may occur temporarily.
  EXPECT_GE(usage.get().processes(), 2U);
  EXPECT_GE(usage.get().threads(), 2U);

  driver.stop();
  driver.join();
}


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


// This tests the create, prepare, isolate and cleanup methods of the
// 'CgroupNetClsIsolatorProcess'. The test first creates a
// 'MesosContainerizer' with net_cls cgroup isolator enabled. The
// net_cls cgroup isolator is implemented in the
// 'CgroupNetClsIsolatorProcess' class. The test then launches a task
// in a mesos container and checks to see if the container has been
// added to the right net_cls cgroup. Finally, the test kills the task
// and makes sure that the 'CgroupNetClsIsolatorProcess' cleans up the
// net_cls cgroup created for the container.
TEST_F(CgroupsIsolatorTest, ROOT_CGROUPS_NET_CLS_Isolate)
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
TEST_F(CgroupsIsolatorTest, ROOT_CGROUPS_NET_CLS_ContainerStatus)
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
  Future<Response> response = process::http::get(
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

} // namespace tests {
} // namespace internal {
} // namespace mesos {
