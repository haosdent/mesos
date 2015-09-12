/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <paths.h>
#include <sched.h>
#include <unistd.h>

#include <gmock/gmock.h>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include <process/gtest.hpp>
#include <process/subprocess.hpp>

#include "linux/fs.hpp"
#include "linux/ns.hpp"
#include "linux/sched.hpp"

#include "tests/utils.hpp"

namespace mesos {
namespace internal {
namespace tests {

using fs::MountTable;
using fs::FileSystemTable;
using fs::MountInfoTable;

using std::string;

using process::Subprocess;
using process::subprocess;

class FsMountTest : public TemporaryDirectoryTest
{
public:
  Try<Subprocess> run(const string& command)
  {
    Try<Subprocess> s = subprocess(
        command,
        Subprocess::PATH("/dev/null"),
        Subprocess::FD(STDOUT_FILENO),
        Subprocess::FD(STDERR_FILENO),
        None(),
        None(),
        lambda::bind(&clone, lambda::_1));

    return s;
  }

private:
  static pid_t clone(const lambda::function<int()>& f)
  {
    static unsigned long long stack[(8*1024*1024)/sizeof(unsigned long long)];

    return ::clone(
        _clone,
        &stack[sizeof(stack)/sizeof(stack[0]) - 1],  // Stack grows down.
        CLONE_NEWNS | SIGCHLD,   // Specify SIGCHLD as child termination signal.
        (void*) &f);
  }

  static int _clone(void* f)
  {
    const lambda::function<int()>* _f =
      static_cast<const lambda::function<int()>*> (f);

    return (*_f)();
  }
};

TEST(FsTest, MountTableRead)
{
  Try<MountTable> table = MountTable::read(_PATH_MOUNTED);

  ASSERT_SOME(table);

  Option<MountTable::Entry> root = None();
  Option<MountTable::Entry> proc = None();
  foreach (const MountTable::Entry& entry, table.get().entries) {
    if (entry.dir == "/") {
      root = entry;
    } else if (entry.dir == "/proc") {
      proc = entry;
    }
  }

  EXPECT_SOME(root);
  ASSERT_SOME(proc);
  EXPECT_EQ(proc.get().type, "proc");
}


TEST(FsTest, MountTableHasOption)
{
  Try<MountTable> table = MountTable::read(_PATH_MOUNTED);

  ASSERT_SOME(table);

  Option<MountTable::Entry> proc = None();
  foreach (const MountTable::Entry& entry, table.get().entries) {
    if (entry.dir == "/proc") {
      proc = entry;
    }
  }

  ASSERT_SOME(proc);
  EXPECT_TRUE(proc.get().hasOption(MNTOPT_RW));
}


TEST(FsTest, FileSystemTableRead)
{
  Try<FileSystemTable> table = FileSystemTable::read();

  ASSERT_SOME(table);

  // NOTE: We do not check for /proc because, it is not always present in
  // /etc/fstab.
  Option<FileSystemTable::Entry> root = None();
  foreach (const FileSystemTable::Entry& entry, table.get().entries) {
    if (entry.file == "/") {
      root = entry;
    }
  }

  EXPECT_SOME(root);
}


TEST(FsTest, MountInfoTableParse)
{
  // Parse a private mount (no optional fields).
  const std::string privateMount =
    "19 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw,seclabel,data=ordered";
  Try<MountInfoTable::Entry> entry = MountInfoTable::Entry::parse(privateMount);

  ASSERT_SOME(entry);
  EXPECT_EQ(19, entry.get().id);
  EXPECT_EQ(1, entry.get().parent);
  EXPECT_EQ(makedev(8, 1), entry.get().devno);
  EXPECT_EQ("/", entry.get().root);
  EXPECT_EQ("/", entry.get().target);
  EXPECT_EQ("rw,relatime", entry.get().vfsOptions);
  EXPECT_EQ("rw,seclabel,data=ordered", entry.get().fsOptions);
  EXPECT_EQ("", entry.get().optionalFields);
  EXPECT_EQ("ext4", entry.get().type);
  EXPECT_EQ("/dev/sda1", entry.get().source);

  // Parse a shared mount (includes one optional field).
  const std::string sharedMount =
    "19 1 8:1 / / rw,relatime shared:2 - ext4 /dev/sda1 rw,seclabel";
  entry = MountInfoTable::Entry::parse(sharedMount);

  ASSERT_SOME(entry);
  EXPECT_EQ(19, entry.get().id);
  EXPECT_EQ(1, entry.get().parent);
  EXPECT_EQ(makedev(8, 1), entry.get().devno);
  EXPECT_EQ("/", entry.get().root);
  EXPECT_EQ("/", entry.get().target);
  EXPECT_EQ("rw,relatime", entry.get().vfsOptions);
  EXPECT_EQ("rw,seclabel", entry.get().fsOptions);
  EXPECT_EQ("shared:2", entry.get().optionalFields);
  EXPECT_EQ("ext4", entry.get().type);
  EXPECT_EQ("/dev/sda1", entry.get().source);
}


TEST(FsTest, DISABLED_MountInfoTableRead)
{
  // Examine the calling process's mountinfo table.
  Try<fs::MountInfoTable> table = fs::MountInfoTable::read();
  ASSERT_SOME(table);

  // Every system should have at least a rootfs mounted.
  Option<MountInfoTable::Entry> root = None();
  foreach (const MountInfoTable::Entry& entry, table.get().entries) {
    if (entry.target == "/") {
      root = entry;
    }
  }

  EXPECT_SOME(root);

  // Repeat for pid 1.
  table = fs::MountInfoTable::read(1);
  ASSERT_SOME(table);

  // Every system should have at least a rootfs mounted.
  root = None();
  foreach (const MountInfoTable::Entry& entry, table.get().entries) {
    if (entry.target == "/") {
      root = entry;
    }
  }

  EXPECT_SOME(root);
}


TEST_F(FsMountTest, ROOT_SharedMount)
{
  Try<Nothing> mount =
    fs::mount(sandbox.get(), sandbox.get(), None(), MS_BIND, NULL);
  ASSERT_SOME(mount)
    << "Failed to mount sandbox '" << sandbox.get() << "': " << mount.error();
  mount = fs::mount(None(), sandbox.get(), None(), MS_SHARED, NULL);
  ASSERT_SOME(mount)
    << "Failed to mark work directory '" << sandbox.get()
    << "' as a shared mount: " << mount.error();
  LOG(INFO) << "Mark '" << sandbox.get() << "' as a shared mount.";

  string source = path::join(sandbox.get(), "source");
  string target = path::join(sandbox.get(), "target");

  Try<Nothing> mkdir = os::mkdir(source);
  ASSERT_SOME(mkdir)
    << "Failed to create dir at '" << source << "': " << mkdir.error();

  mkdir = os::mkdir(target);
  ASSERT_SOME(mkdir)
    << "Failed to create persistent volume mount point at '" << target << "': "
    << mkdir.error();

  mount = fs::mount(source, target, None(), MS_BIND, NULL);
  ASSERT_SOME(mount)
    << "Failed to mount persistent volume from '" << source << "' to '"
    << target << "': " << mount.error();
  LOG(INFO) << "Mounting '" << source << "' to '" << target;

  // Although use shared mount, also could not slove this problem.
  // mount = fs::mount(None(), target, None(), MS_SHARED, NULL);
  // ASSERT_SOME(mount)
  //   << "Failed to mark work directory '" << target << "' as a shared mount: "
  //   << mount.error();
  // LOG(INFO) << "Mark '" << target << "' as a shared mount.";

  Try<Subprocess> s = run("sleep 2");

  Try<Nothing> unmount = fs::unmount(target);
  ASSERT_SOME(unmount)
    << "Failed to unmount '" << target << "': " << unmount.error();
  LOG(INFO) << "Unmount '" << target << "'";

  LOG(INFO) << "After Unmount, current process mount table: "
    << os::read("/proc/self/mountinfo").get();
  LOG(INFO) << "After Unmount, child mount table: "
    << os::read("/proc/" + stringify(s.get().pid()) + "/mountinfo").get();

  Try<Nothing> rmdir = os::rmdir(target, false);
  EXPECT_SOME(rmdir) << "Failed to rmdir target: " << rmdir.error();
  rmdir = os::rmdir(source, false);
  EXPECT_SOME(rmdir) << "Failed to rmdir source: " << rmdir.error();

  AWAIT_READY(s.get().status());

  unmount = fs::unmount(sandbox.get());
  ASSERT_SOME(unmount)
    << "Failed to unmount '" << sandbox.get() << "': " << unmount.error();
  LOG(INFO) << "Unmount '" << sandbox.get() << "'";
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
