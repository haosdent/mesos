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

#ifndef __HEALTH_CHECKER_HPP__
#define __HEALTH_CHECKER_HPP__

#include <signal.h>
#include <stdio.h>
#include <string.h>
#ifndef __WINDOWS__
#include <unistd.h>
#endif // __WINDOWS__

#include <iostream>
#include <string>
#include <tuple>

#include <mesos/mesos.hpp>

#include <process/collect.hpp>
#include <process/delay.hpp>
#include <process/future.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/io.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/subprocess.hpp>

#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>
#include <stout/strings.hpp>

#include <stout/os/killtree.hpp>

#include "common/status_utils.hpp"

#include "messages/messages.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::map;
using std::string;
using std::tuple;

using process::UPID;

namespace mesos {
namespace internal {

// Forward declarations.
class HealthCheckerProcess;

class HealthChecker
{
public:
  static Try<process::Owned<HealthChecker>> create(
      const HealthCheck& check,
      const UPID& executor,
      const TaskID& taskID);

  ~HealthChecker();

  process::Future<Nothing> healthCheck();

private:
  explicit HealthChecker(process::Owned<HealthCheckerProcess> process);

  process::Owned<HealthCheckerProcess> process;
};


class HealthCheckerProcess : public ProtobufProcess<HealthCheckerProcess>
{
public:
  HealthCheckerProcess(
      const HealthCheck& _check,
      const UPID& _executor,
      const TaskID& _taskID)
    : ProcessBase(process::ID::generate("health-checker")),
      check(_check),
      initializing(true),
      executor(_executor),
      taskID(_taskID),
      consecutiveFailures(0) {}

  virtual ~HealthCheckerProcess() {}

  process::Future<Nothing> healthCheck()
  {
    VLOG(2) << "Health checks starting in "
      << Seconds(check.delay_seconds()) << ", grace period "
      << Seconds(check.grace_period_seconds());

    startTime = process::Clock::now();

    delay(Seconds(check.delay_seconds()), self(), &Self::_healthCheck);
    return promise.future();
  }

private:
  void failure(const string& message)
  {
    if (check.grace_period_seconds() > 0 &&
        (process::Clock::now() - startTime).secs() <=
          check.grace_period_seconds()) {
      LOG(INFO) << "Ignoring failure as health check still in grace period";
      reschedule();
      return;
    }

    consecutiveFailures++;
    VLOG(1) << "#" << consecutiveFailures << " check failed: " << message;

    bool killTask = consecutiveFailures >= check.consecutive_failures();

    TaskHealthStatus taskHealthStatus;
    taskHealthStatus.set_healthy(false);
    taskHealthStatus.set_consecutive_failures(consecutiveFailures);
    taskHealthStatus.set_kill_task(killTask);
    taskHealthStatus.mutable_task_id()->CopyFrom(taskID);
    send(executor, taskHealthStatus);

    if (killTask) {
      // This is a hack to ensure the message is sent to the
      // executor before we exit the process. Without this,
      // we may exit before libprocess has sent the data over
      // the socket. See MESOS-4111.
      os::sleep(Seconds(1));
      promise.fail(message);
    } else {
      reschedule();
    }
  }

  void success()
  {
    VLOG(1) << "Check passed";

    // Send a healthy status update on the first success,
    // and on the first success following failure(s).
    if (initializing || consecutiveFailures > 0) {
      TaskHealthStatus taskHealthStatus;
      taskHealthStatus.set_healthy(true);
      taskHealthStatus.mutable_task_id()->CopyFrom(taskID);
      send(executor, taskHealthStatus);
      initializing = false;
    }
    consecutiveFailures = 0;
    reschedule();
  }

  void _commandHealthCheck()
  {
    const CommandInfo& command = check.command();

    map<string, string> environment = os::environment();

    foreach (const Environment::Variable& variable,
             command.environment().variables()) {
      environment[variable.name()] = variable.value();
    }

    // Launch the subprocess.
    Option<Try<process::Subprocess>> external = None();

    if (command.shell()) {
      // Use the shell variant.
      if (!command.has_value()) {
        promise.fail("Shell command is not specified");
        return;
      }

      VLOG(2) << "Launching health command '" << command.value() << "'";

      external = process::subprocess(
          command.value(),
          process::Subprocess::PATH("/dev/null"),
          process::Subprocess::FD(STDERR_FILENO),
          process::Subprocess::FD(STDERR_FILENO),
          process::NO_SETSID,
          environment);
    } else {
      // Use the exec variant.
      if (!command.has_value()) {
        promise.fail("Executable path is not specified");
        return;
      }

      vector<string> argv;
      foreach (const string& arg, command.arguments()) {
        argv.push_back(arg);
      }

      VLOG(2) << "Launching health command [" << command.value() << ", "
              << strings::join(", ", argv) << "]";

      external = process::subprocess(
          command.value(),
          argv,
          process::Subprocess::PATH("/dev/null"),
          process::Subprocess::FD(STDERR_FILENO),
          process::Subprocess::FD(STDERR_FILENO),
          process::NO_SETSID,
          None(),
          environment);
    }

    CHECK_SOME(external);

    if (external.get().isError()) {
      failure("Error creating subprocess for healthcheck: " +
              external.get().error());
      return;
    }

    pid_t commandPid = external.get().get().pid();

    process::Future<Option<int>> status = external.get().get().status();
    status.await(Seconds(check.timeout_seconds()));

    if (!status.isReady()) {
      string msg = "Command check failed with reason: ";
      if (status.isFailed()) {
        msg += "failed with error: " + status.failure();
      } else if (status.isDiscarded()) {
        msg += "status future discarded";
      } else {
        msg += "status still pending after timeout " +
               stringify(Seconds(check.timeout_seconds()));
      }

      if (commandPid != -1) {
        // Cleanup the external command process.
        os::killtree(commandPid, SIGKILL);
        VLOG(1) << "Kill health check command " << commandPid;
      }

      failure(msg);
      return;
    }

    int statusCode = status.get().get();
    if (statusCode != 0) {
      string message = "Health command check " + WSTRINGIFY(statusCode);
      failure(message);
    } else {
      success();
    }
  }

  void _httpHealthCheck()
  {
    const HealthCheck_HTTP& http = check.http();

    string scheme = http.has_scheme() ? http.scheme() : "http";
    if (scheme != "https" && scheme != "http") {
      promise.fail("Unsupported HTTP health check scheme: '" + scheme + "'");
    }

    string path = http.has_path() ? http.path() : "/";

    string url = scheme + "://localhost:" + stringify(http.port()) + path;

    VLOG(2) << "Launching HTTP health check '" << url << "'";

    const vector<string> argv = {
      "curl",
      "-s",                 // Don't show progress meter or error messages.
      "-S",                 // Makes curl show an error message if it fails.
      "-L",                 // Follow HTTP 3xx redirects.
      "-w", "%{http_code}", // Display HTTP response code on stdout.
      "-o", "/dev/null",    // Ignores output.
      url
    };

    Try<process::Subprocess> s = process::subprocess(
        "curl",
        argv,
        process::Subprocess::PATH("/dev/null"),
        process::Subprocess::PIPE(),
        process::Subprocess::PIPE());

    if (s.isError()) {
      failure("HTTP health check failed with reason: "
              "Failed to exec the curl subprocess: " + s.error());
    }

    Duration timeout = Seconds(check.timeout_seconds());

    await(
        s.get().status(),
        process::io::read(s.get().out().get()),
        process::io::read(s.get().err().get()))
      .after(timeout,
             defer(self(), &Self::__httpHealthCheck, timeout, lambda::_1))
      .then(defer(self(), &Self::___httpHealthCheck, lambda::_1))
      .onAny(defer(self(), &Self::____httpHealthCheck, lambda::_1));
  }

  process::Future<tuple<
      process::Future<Option<int>>,
      process::Future<string>,
      process::Future<string>>> __httpHealthCheck(
          const Duration& timeout,
          process::Future<tuple<
              process::Future<Option<int>>,
              process::Future<string>,
              process::Future<string>>> future)
  {
    future.discard();
    return process::Failure(
        "Query still pending after timeout " + stringify(timeout));
  }

  process::Future<Nothing> ___httpHealthCheck(
      const tuple<
          process::Future<Option<int>>,
          process::Future<string>,
          process::Future<string>>& t)
  {
    process::Future<Option<int>> status = std::get<0>(t);
    if (!status.isReady()) {
      return process::Failure(
          "Failed to get the exit status of the curl subprocess: " +
          (status.isFailed() ? status.failure() : "discarded"));
    }

    if (status->isNone()) {
      return process::Failure("Failed to reap the curl subprocess");
    }

    if (status->get() != 0) {
      process::Future<string> error = std::get<2>(t);
      if (!error.isReady()) {
        return process::Failure(
            "Failed to perform 'curl': Reading stderr failed: " +
            (error.isFailed() ? error.failure() : "discarded"));
      }

      return process::Failure("Failed to perform 'curl': " + error.get());
    }

    process::Future<string> output = std::get<1>(t);
    if (!output.isReady()) {
      return process::Failure(
          "Failed to read stdout from 'curl': " +
          (output.isFailed() ? output.failure() : "discarded"));
    }

    // Parse the output and get the HTTP response code.
    Try<int> code = numify<int>(output.get());
    if (code.isError()) {
      return process::Failure("Unexpected output from 'curl': " + output.get());
    }

    if (code.get() >= process::http::Status::BAD_REQUEST ||
        code.get() < process::http::Status::OK) {
      return process::Failure(
          "Unexpected HTTP response code: " +
          process::http::Status::string(code.get()));
    }

    return Nothing();
  }

  void ____httpHealthCheck(const process::Future<Nothing>& future)
  {
    if (future.isReady()) {
      success();
    }

    string msg = "HTTP health check failed with reason: " +
                 (future.isFailed() ? future.failure() : "discarded");

    failure(msg);
  }

  void _healthCheck()
  {
    if (check.type() == HealthCheck::COMMAND_CHECK) {
      _commandHealthCheck();
    } else if (check.type() == HealthCheck::HTTP_CHECK) {
      _httpHealthCheck();
    }
  }

  void reschedule()
  {
    VLOG(1) << "Rescheduling health check in "
            << Seconds(check.interval_seconds());

    delay(Seconds(check.interval_seconds()), self(), &Self::_healthCheck);
  }

  process::Promise<Nothing> promise;
  HealthCheck check;
  bool initializing;
  UPID executor;
  TaskID taskID;
  uint32_t consecutiveFailures;
  process::Time startTime;
};

} // namespace internal {
} // namespace mesos {

#endif // __HEALTH_CHECKER_HPP__
