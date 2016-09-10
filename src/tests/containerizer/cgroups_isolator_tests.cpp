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

#include "slave/containerizer/mesos/containerizer.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/constants.hpp"

#include "tests/mesos.hpp"
#include "tests/script.hpp"

using mesos::internal::master::Master;

using mesos::internal::slave::CGROUP_SUBSYSTEM_CPU_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_CPUACCT_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_DEVICES_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_MEMORY_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_NET_CLS_NAME;
using mesos::internal::slave::CGROUP_SUBSYSTEM_PERF_EVENT_NAME;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;

using mesos::master::detector::MasterDetector;

using process::Future;
using process::Owned;

using std::set;
using std::string;
using std::vector;

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

} // namespace tests {
} // namespace internal {
} // namespace mesos {
