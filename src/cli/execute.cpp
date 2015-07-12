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

#include <iostream>
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <mesos/type_utils.hpp>

#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/http.hpp>
#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/flags.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/try.hpp>

#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"

#include "hdfs/hdfs.hpp"

using namespace mesos;
using namespace mesos::internal;

using process::delay;
using process::dispatch;
using process::Future;
using process::PID;
using process::Process;
using process::UPID;

using process::http::Response;
using process::http::statuses;

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;


class Flags : public flags::FlagsBase
{
public:
  Flags()
  {
    add(&master,
        "master",
        "Mesos master (e.g., IP1:PORT1)");

    add(&name,
        "name",
        "Name for the command");

    add(&command,
        "command",
        "Shell command to launch");

    add(&environment,
        "env",
        "Shell command environment variables.\n"
        "The value could be a JSON formatted string of environment variables"
        "(ie: {\"name1\": \"value1\"} )\n"
        "or a file path containing the JSON formatted environment variables.\n"
        "Path could be of the form 'file:///path/to/file' "
        "or '/path/to/file'.\n");

    add(&resources,
        "resources",
        "Resources for the command",
        "cpus:1;mem:128");

    add(&hadoop,
        "hadoop",
        "Path to `hadoop' script (used for copying packages)",
        "hadoop");

    add(&hdfs,
        "hdfs",
        "The ip:port of the NameNode service",
        "localhost:9000");

    add(&package,
        "package",
        "Package to upload into HDFS and copy into command's\n"
        "working directory (requires `hadoop', see --hadoop)");

    add(&overwrite,
        "overwrite",
        "Overwrite the package in HDFS if it already exists",
        false);

    add(&checkpoint,
        "checkpoint",
        "Enable checkpointing for the framework (requires slave checkpointing)",
        false);

    add(&docker_image,
        "docker_image",
        "Docker image that follows the Docker CLI naming <image>:<tag>."
        "(ie: ubuntu, busybox:latest).");
  }

  Option<string> master;
  Option<string> name;
  Option<string> command;
  Option<hashmap<string, string>> environment;
  string resources;
  string hadoop;
  string hdfs;
  Option<string> package;
  bool overwrite;
  bool checkpoint;
  Option<string> docker_image;
};


class PollingOutputProcess : public Process<PollingOutputProcess>
{
public:
  PollingOutputProcess(const string& _taskName)
    : taskName(_taskName),
      stdoutOffset(0),
      stderrOffset(0),
      pollingInterval(Seconds(2)),
      httpTimeout(Seconds(5)) {}

  virtual ~PollingOutputProcess() {}

  void polling() {
    delay(pollingInterval, self(), &PollingOutputProcess::polling);

    Try<UPID> slave = getSlavePid();
    if (slave.isError()) {
      return;
    }

    Try<string> directory = getSandboxDirectory(slave.get());
    if (directory.isError()) {
      return;
    }
    process::UPID filesUpid("files", slave.get().address);

    string stdout = path::join(directory.get(), "stdout");
    while (true) {
      string params = "path=" + stdout +"&offset=" + stringify(stdoutOffset);
      Try<string> output = _polling(filesUpid, params);
      if (output.isError()) {
        break;
      }
      if (output.get().length() > 0) {
        stdoutOffset += output.get().length();
        cout << output.get();
      } else {
        break;
      }
    }

    string stderr = path::join(directory.get(), "stderr");
    while (true) {
      string params = "path=" + stderr +"&offset=" + stringify(stderrOffset);
      Try<string> output = _polling(filesUpid, params);
      if (output.isError()) {
        break;
      }
      if (output.get().length() > 0) {
          stderrOffset += output.get().length();
          cerr << output.get();
      } else {
        break;
      }
    }
  }

  void setMasterInfo(const MasterInfo& _masterInfo)
  {
    masterInfo = _masterInfo;
  }

  void setFrameworkId(const FrameworkID& _frameworkId)
  {
    frameworkId = _frameworkId;
  }

private:
  Try<UPID> getSlavePid()
  {
    UPID master(masterInfo.pid());
    if (!master) {
      return Error("Invalid masterInfo.");
    }
    Future<Response> response = process::http::get(master, "state.json");
    if (!response.await(Seconds(5))) {
      return Error("Timed out when request master's state.json.");
    }
    Try<JSON::Object> parse = JSON::parse<JSON::Object>(response.get().body);
    if (parse.isError()) {
      return Error("Parse master's state.json failed: " + parse.error());
    }

    JSON::Object& state = parse.get();
    JSON::Array frameworks = state.values["frameworks"].as<JSON::Array>();
    JSON::Object framework;
    bool isFounded = false;
    foreach (const JSON::Value& tmpFrameworkValue, frameworks.values) {
      const JSON::Object& tmpFramework = tmpFrameworkValue.as<JSON::Object>();
      string tmpFrameworkId =
        tmpFramework.values.at("id").as<JSON::String>().value;
      if (tmpFrameworkId == frameworkId.value()) {
        framework = tmpFramework;
        isFounded = true;
        break;
      }
    }
    if (!isFounded) {
      return Error("Could not found framework '" + frameworkId.value() +
                   "' in master's state.json.");
    }
    if (!framework.values.count("tasks")) {
      return Error("tasks in framework(" + frameworkId.value() +
                   ") is not exists.");
    }

    string slaveId;
    isFounded = false;
    foreach (const JSON::Value& tmpTaskValue,
             framework.values["tasks"].as<JSON::Array>().values) {
      const JSON::Object& tmpTask = tmpTaskValue.as<JSON::Object>();
      string tmpTaskId =
        tmpTask.values.at("id").as<JSON::String>().value;
      if (tmpTaskId == taskName) {
        slaveId = tmpTask.values.at("slave_id").as<JSON::String>().value;
        isFounded = true;
        break;
      }
    }
    if (!isFounded) {
      return Error("Could not found task '" + taskName +
                   "' in master's state.json.");
    }
    if (slaveId.empty()) {
      return Error("slave_id in task('" + taskName + "') is empty.");
    }

    string slavePid;
    isFounded = false;
    foreach (const JSON::Value& tmpSlaveValue,
             state.values["slaves"].as<JSON::Array>().values) {
      const JSON::Object& tmpSlave = tmpSlaveValue.as<JSON::Object>();
      string tmpSlaveId = tmpSlave.values.at("id").as<JSON::String>().value;
      if (tmpSlaveId == slaveId) {
        slavePid = tmpSlave.values.at("pid").as<JSON::String>().value;
        isFounded = true;
        break;
      }
    }
    if (!isFounded) {
      return Error("Could not found slave '" + slaveId +
                   "' in master's state.json.");
    }

    UPID slave(slavePid);
    if (!slave) {
      return Error("Invalid slave pid: " + slavePid);
    }

    return slave;
  }

  Try<string> getSandboxDirectory(UPID& slave)
  {
    Future<Response> response = process::http::get(slave, "state.json");
    if (!response.await(Seconds(5))) {
      return Error("Timed out when request slave's state.json.");
    }
    Try<JSON::Object> parse = JSON::parse<JSON::Object>(response.get().body);
    if (parse.isError()) {
      return Error("Parse slave's state.json failed: " + parse.error());
    }

    JSON::Object& state = parse.get();
    JSON::Array frameworks = state.values["frameworks"].as<JSON::Array>();
    JSON::Object framework;
    bool isFounded = false;
    foreach (const JSON::Value& tmpFrameworkValue, frameworks.values) {
      const JSON::Object& tmpFramework = tmpFrameworkValue.as<JSON::Object>();
      string tmpFrameworkId =
        tmpFramework.values.at("id").as<JSON::String>().value;
      if (tmpFrameworkId == frameworkId.value()) {
        framework = tmpFramework;
        isFounded = true;
        break;
      }
    }
    if (!isFounded) {
      return Error("Could not found framework '" + frameworkId.value() +
                   "' in slave's state.json.");
    }
    if (!framework.values.count("executors")) {
      return Error("executors in framework(" + frameworkId.value() +
                   ") is not exists.");
    }

    string directory;
    isFounded = false;
    foreach (const JSON::Value& tmpExecutorValue,
             framework.values["executors"].as<JSON::Array>().values) {
      const JSON::Object& tmpExecutor = tmpExecutorValue.as<JSON::Object>();
      string tmpExecutorId =
        tmpExecutor.values.at("id").as<JSON::String>().value;
      if (tmpExecutorId == taskName) {
        directory = tmpExecutor.values.at("directory").as<JSON::String>().value;
        isFounded = true;
        break;
      }
    }
    if (!isFounded) {
      return Error("Could not found executor '" + taskName +
                   "' in slave's state.json.");
    }
    if (directory.empty()) {
      return Error("directory in task('" + taskName + "') is empty.");
    }

    return directory;
  }

  Try<string> _polling(const UPID& filesUpid, const string& params)
  {
    Future<Response> response =
      process::http::get(filesUpid, "read.json", params);
    if (!response.await(Seconds(5))) {
      return Error("Timed out when request slave's read.json.");
    }

    if (response.get().status != statuses[200]) {
      return Error(
          "Invalid read.json response status: " + response.get().status);
    }

    Try<JSON::Object> parse = JSON::parse<JSON::Object>(response.get().body);
    if (parse.isError()) {
      return Error("Parse slave's read.json failed: " + parse.error());
    }
    JSON::Object& readResponse = parse.get();

    return readResponse.values.at("data").as<JSON::String>().value;
  }

  string taskName;
  MasterInfo masterInfo;
  FrameworkID frameworkId;
  int stdoutOffset;
  int stderrOffset;
  Duration pollingInterval;
  Duration httpTimeout;
};


class CommandScheduler : public Scheduler
{
public:
  CommandScheduler(
      const string& _name,
      const string& _command,
      const Option<hashmap<string, string>>& _environment,
      const string& _resources,
      const Option<string>& _uri,
      const Option<string>& _dockerImage)
    : name(_name),
      command(_command),
      environment(_environment),
      resources(_resources),
      uri(_uri),
      dockerImage(_dockerImage),
      launched(false),
      pollingOutputProcess(_name) {}

  virtual ~CommandScheduler() {}

  virtual void registered(
      SchedulerDriver* _driver,
      const FrameworkID& _frameworkId,
      const MasterInfo& _masterInfo) {
    pollingOutputProcess.setMasterInfo(_masterInfo);
    pollingOutputProcess.setFrameworkId(_frameworkId);
    cout << "Framework registered with " << _frameworkId << endl;
  }

  virtual void reregistered(
      SchedulerDriver* _driver,
      const MasterInfo& _masterInfo) {
    pollingOutputProcess.setMasterInfo(_masterInfo);
    cout << "Framework re-registered" << endl;
  }

  virtual void disconnected(
      SchedulerDriver* driver) {}

  virtual void resourceOffers(
      SchedulerDriver* driver,
      const vector<Offer>& offers)
  {
    static const Try<Resources> TASK_RESOURCES = Resources::parse(resources);

    if (TASK_RESOURCES.isError()) {
      cerr << "Failed to parse resources '" << resources
           << "': " << TASK_RESOURCES.error() << endl;
      driver->abort();
      return;
    }

    foreach (const Offer& offer, offers) {
      if (!launched &&
          Resources(offer.resources()).contains(TASK_RESOURCES.get())) {
        TaskInfo task;
        task.set_name(name);
        task.mutable_task_id()->set_value(name);
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_resources()->CopyFrom(TASK_RESOURCES.get());

        CommandInfo* commandInfo = task.mutable_command();
        commandInfo->set_value(command);
        if (environment.isSome()) {
          Environment* environment_ = commandInfo->mutable_environment();
          foreachpair (const std::string& name,
                       const std::string& value,
                       environment.get()) {
            Environment_Variable* environmentVariable =
              environment_->add_variables();
            environmentVariable->set_name(name);
            environmentVariable->set_value(value);
          }
        }

        if (uri.isSome()) {
          task.mutable_command()->add_uris()->set_value(uri.get());
        }

        if (dockerImage.isSome()) {
          ContainerInfo containerInfo;
          containerInfo.set_type(ContainerInfo::DOCKER);

          ContainerInfo::DockerInfo dockerInfo;
          dockerInfo.set_image(dockerImage.get());

          containerInfo.mutable_docker()->CopyFrom(dockerInfo);
          task.mutable_container()->CopyFrom(containerInfo);
        }

        vector<TaskInfo> tasks;
        tasks.push_back(task);

        driver->launchTasks(offer.id(), tasks);
        cout << "task " << name << " submitted to slave "
             << offer.slave_id() << endl;
        pollingOutputPid = spawn(&pollingOutputProcess);
        launched = true;
      } else {
        driver->declineOffer(offer.id());
      }
    }
  }

  virtual void offerRescinded(
      SchedulerDriver* driver,
      const OfferID& offerId) {}

  virtual void statusUpdate(
      SchedulerDriver* driver,
      const TaskStatus& status)
  {
    CHECK_EQ(name, status.task_id().value());
    cout << "Received status update " << status.state()
         << " for task " << status.task_id() << endl;
    dispatch(pollingOutputPid, &PollingOutputProcess::polling);
    if (mesos::internal::protobuf::isTerminalState(status.state())) {
      driver->stop();
    }
  }

  virtual void frameworkMessage(
      SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      const string& data) {}

  virtual void slaveLost(
      SchedulerDriver* driver,
      const SlaveID& sid) {}

  virtual void executorLost(
      SchedulerDriver* driver,
      const ExecutorID& executorID,
      const SlaveID& slaveID,
      int status) {}

  virtual void error(
      SchedulerDriver* driver,
      const string& message) {}

private:
  const string name;
  const string command;
  const Option<hashmap<string, string>> environment;
  const string resources;
  const Option<string> uri;
  const Option<string> dockerImage;
  bool launched;
  PollingOutputProcess pollingOutputProcess;
  PID<PollingOutputProcess> pollingOutputPid;
};


int main(int argc, char** argv)
{
  Flags flags;

  // Load flags from environment and command line.
  Try<Nothing> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  // TODO(marco): this should be encapsulated entirely into the
  // FlagsBase API - possibly with a 'guard' that prevents FlagsBase
  // from calling ::exit(EXIT_FAILURE) after calling usage() (which
  // would be the default behavior); see MESOS-2766.
  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.master.isNone()) {
    cerr << flags.usage("Missing required option --master") << endl;
    return EXIT_FAILURE;
  }

  UPID master("master@" + flags.master.get());
  if (!master) {
    cerr << flags.usage("Could not parse --master=" + flags.master.get())
         << endl;
    return EXIT_FAILURE;
  }

  if (flags.name.isNone()) {
    cerr << flags.usage("Missing required option --name") << endl;
    return EXIT_FAILURE;
  }

  if (flags.command.isNone()) {
    cerr << flags.usage("Missing required option --command") << endl;
    return EXIT_FAILURE;
  }

  Result<string> user = os::user();
  if (!user.isSome()) {
    if (user.isError()) {
      cerr << "Failed to get username: " << user.error() << endl;
    } else {
      cerr << "No username for uid " << ::getuid() << endl;
    }
    return EXIT_FAILURE;
  }

  Option<hashmap<string, string>> environment = None();

  if (flags.environment.isSome()) {
    environment = flags.environment.get();
  }

  // Copy the package to HDFS if requested save it's location as a URI
  // for passing to the command (in CommandInfo).
  Option<string> uri = None();

  if (flags.package.isSome()) {
    HDFS hdfs(flags.hadoop);

    // TODO(benh): If HDFS is not properly configured with
    // 'fs.default.name' then we'll copy to the local
    // filesystem. Currently this will silently fail on our end (i.e.,
    // the 'copyFromLocal' will be successful) but we'll fail to
    // download the URI when we launch the executor (unless it's
    // already been uploaded before ...).

    // Store the file at '/user/package'.
    string path = path::join("/", user.get(), flags.package.get());

    // Check if the file exists and remove it if we're overwriting.
    Try<bool> exists = hdfs.exists(path);
    if (exists.isError()) {
      cerr << "Failed to check if file exists: " << exists.error() << endl;
      return EXIT_FAILURE;
    } else if (exists.get() && flags.overwrite) {
      Try<Nothing> rm = hdfs.rm(path);
      if (rm.isError()) {
        cerr << "Failed to remove existing file: " << rm.error() << endl;
        return EXIT_FAILURE;
      }
    } else if (exists.get()) {
      cerr << "File already exists (see --overwrite)" << endl;
      return EXIT_FAILURE;
    }

    Try<Nothing> copy = hdfs.copyFromLocal(flags.package.get(), path);
    if (copy.isError()) {
      cerr << "Failed to copy package: " << copy.error() << endl;
      return EXIT_FAILURE;
    }

    // Now save the URI.
    uri = "hdfs://" + flags.hdfs + path;
  }

  Option<string> dockerImage;

  if (flags.docker_image.isSome()) {
    dockerImage = flags.docker_image.get();
  }

  CommandScheduler scheduler(
      flags.name.get(),
      flags.command.get(),
      environment,
      flags.resources,
      uri,
      dockerImage);

  FrameworkInfo framework;
  framework.set_user(user.get());
  framework.set_name("");
  framework.set_checkpoint(flags.checkpoint);

  MesosSchedulerDriver driver(&scheduler, framework, flags.master.get());

  return driver.run() == DRIVER_STOPPED ? EXIT_SUCCESS : EXIT_FAILURE;
}
