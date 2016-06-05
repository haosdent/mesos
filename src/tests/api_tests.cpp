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

#include <mesos/v1/master.hpp>

#include <mesos/http.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/gtest.hpp>
#include <stout/jsonify.hpp>
#include <stout/nothing.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/http.hpp"
#include "common/protobuf_utils.hpp"

#include "master/detector/standalone.hpp"

#include "slave/slave.hpp"

#include "tests/mesos.hpp"

using mesos::master::detector::StandaloneMasterDetector;

using mesos::internal::slave::Slave;

using mesos::internal::protobuf::maintenance::createSchedule;
using mesos::internal::protobuf::maintenance::createUnavailability;
using mesos::internal::protobuf::maintenance::createWindow;

using process::Clock;
using process::Failure;
using process::Future;
using process::Owned;

using process::http::Accepted;
using process::http::OK;
using process::http::Response;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Return;
using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {

class MasterAPITest
  : public MesosTest,
    public WithParamInterface<ContentType>
{
protected:
  // Helper function to post a request to "/api/v1" master endpoint.
  Future<Response> _post(
      const process::PID<master::Master>& pid,
      const v1::master::Call& call,
      const ContentType& contentType)
  {
    process::http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    headers["Accept"] = stringify(contentType);

    return process::http::post(
        pid,
        "api/v1",
        headers,
        serialize(contentType, call),
        stringify(contentType));
  }

public:
  // Helper function to post a request to "/api/v1" master endpoint and return
  // the response.
  Future<v1::master::Response> post(
      const process::PID<master::Master>& pid,
      const v1::master::Call& call,
      const ContentType& contentType)
  {
    return _post(
        pid,
        call,
        contentType)
      .then([contentType](const Response& response)
            -> Future<v1::master::Response> {
        if (response.status != OK().status) {
          return Failure("Unexpected response status " + response.status);
        }
        return deserialize<v1::master::Response>(contentType, response.body);
      });
  }

  // Helper function to post a request to "/api/v1" master endpoint and return
  // `Accepted`.
  Future<Nothing> postWithoutResponse(
      const process::PID<master::Master>& pid,
      const v1::master::Call& call,
      const ContentType& contentType)
  {
    return _post(
        pid,
        call,
        contentType)
      .then([contentType](const Response& response) -> Future<Nothing> {
        if (response.status != Accepted().status) {
          return Failure("Unexpected response status " + response.status);
        }
        return Nothing();
      });
  }
};


// These tests are parameterized by the content type of the HTTP request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    MasterAPITest,
    ::testing::Values(ContentType::PROTOBUF, ContentType::JSON));


TEST_P(MasterAPITest, GetFlags)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call v1Call;
  v1Call.set_type(v1::master::Call::GET_FLAGS);

  ContentType contentType = GetParam();

  Future<v1::master::Response> v1Response =
    post(master.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::master::Response::GET_FLAGS, v1Response.get().type());
}


TEST_P(MasterAPITest, GetHealth)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call v1Call;
  v1Call.set_type(v1::master::Call::GET_HEALTH);

  ContentType contentType = GetParam();

  Future<v1::master::Response> v1Response =
    post(master.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::master::Response::GET_HEALTH, v1Response.get().type());
  ASSERT_TRUE(v1Response.get().get_health().healthy());
}


TEST_P(MasterAPITest, GetVersion)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call v1Call;
  v1Call.set_type(v1::master::Call::GET_VERSION);

  ContentType contentType = GetParam();

  Future<v1::master::Response> v1Response =
    post(master.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::master::Response::GET_VERSION, v1Response.get().type());

  ASSERT_EQ(MESOS_VERSION,
            v1Response.get().get_version().version_info().version());
}


TEST_P(MasterAPITest, GetLoggingLevel)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call v1Call;
  v1Call.set_type(v1::master::Call::GET_LOGGING_LEVEL);

  ContentType contentType = GetParam();

  Future<v1::master::Response> v1Response =
    post(master.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::master::Response::GET_LOGGING_LEVEL, v1Response.get().type());
  ASSERT_LE(0, FLAGS_v);
  ASSERT_EQ(
      v1Response.get().get_logging_level().level(),
      static_cast<uint32_t>(FLAGS_v));
}


TEST_P(MasterAPITest, GetLeadingMaster)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call v1Call;
  v1Call.set_type(v1::master::Call::GET_LEADING_MASTER);

  ContentType contentType = GetParam();

  Future<v1::master::Response> v1Response =
    post(master.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response->IsInitialized());
  ASSERT_EQ(v1::master::Response::GET_LEADING_MASTER, v1Response->type());
  ASSERT_EQ(master.get()->getMasterInfo().ip(),
            v1Response->get_leading_master().master_info().ip());
}


// Test updates a maintenance schedule and verifies it saved via query.
TEST_P(MasterAPITest, UpdateAndGetMaintenanceSchedule)
{
  // Set up a master.
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  ContentType contentType = GetParam();

  // Generate `MachineID`s that can be used in this test.
  MachineID machine1;
  MachineID machine2;
  machine1.set_hostname("Machine1");
  machine2.set_ip("0.0.0.2");

  // Try to schedule maintenance on an unscheduled machine.
  v1::maintenance::Schedule schedule;
  maintenance::Schedule _schedule = createSchedule(
      {createWindow({machine1, machine2}, createUnavailability(Clock::now()))});

  // Convert to v1 message.
  string scheduleData;
  _schedule.SerializePartialToString(&scheduleData);
  schedule.ParsePartialFromString(scheduleData);

  v1::master::Call v1UpdateScheduleCall;
  v1UpdateScheduleCall.set_type(v1::master::Call::UPDATE_MAINTENANCE_SCHEDULE);
  v1::master::Call_UpdateMaintenanceSchedule* maintenanceSchedule =
    v1UpdateScheduleCall.mutable_update_maintenance_schedule();
  maintenanceSchedule->mutable_schedule()->CopyFrom(schedule);

  Future<Nothing> v1UpdateScheduleResponse =
    postWithoutResponse(master.get()->pid, v1UpdateScheduleCall, contentType);

  AWAIT_READY(v1UpdateScheduleResponse);

  // Query maintenance schedule.
  v1::master::Call v1GetScheduleCall;
  v1GetScheduleCall.set_type(v1::master::Call::GET_MAINTENANCE_SCHEDULE);

  Future<v1::master::Response> v1GetScheduleResponse =
    post(master.get()->pid, v1GetScheduleCall, contentType);

  AWAIT_READY(v1GetScheduleResponse);
  ASSERT_TRUE(v1GetScheduleResponse.get().IsInitialized());
  ASSERT_EQ(
      v1::master::Response::GET_MAINTENANCE_SCHEDULE,
      v1GetScheduleResponse.get().type());

  // Verify maintenance schedule matches the expectation.
  v1::maintenance::Schedule respSchedule =
    v1GetScheduleResponse.get().get_maintenance_schedule().schedule();
  string respScheduleData;
  respSchedule.SerializePartialToString(&respScheduleData);
  ASSERT_EQ(scheduleData, respScheduleData);
}


// Test queries for machine maintenance status.
TEST_P(MasterAPITest, GetMaintenanceStatus)
{
  // Set up a master.
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  ContentType contentType = GetParam();

  // Generate `MachineID`s that can be used in this test.
  MachineID machine1;
  MachineID machine2;
  machine1.set_hostname("Machine1");
  machine2.set_ip("0.0.0.2");

  // Try to schedule maintenance on an unscheduled machine.
  v1::maintenance::Schedule schedule;
  maintenance::Schedule _schedule = createSchedule(
      {createWindow({machine1, machine2}, createUnavailability(Clock::now()))});

  // Convert to v1 message.
  string scheduleData;
  _schedule.SerializePartialToString(&scheduleData);
  schedule.ParsePartialFromString(scheduleData);

  v1::master::Call v1UpdateScheduleCall;
  v1UpdateScheduleCall.set_type(v1::master::Call::UPDATE_MAINTENANCE_SCHEDULE);
  v1::master::Call_UpdateMaintenanceSchedule* maintenanceSchedule =
    v1UpdateScheduleCall.mutable_update_maintenance_schedule();
  maintenanceSchedule->mutable_schedule()->CopyFrom(schedule);

  Future<Nothing> v1UpdateScheduleResponse =
    postWithoutResponse(master.get()->pid, v1UpdateScheduleCall, contentType);

  AWAIT_READY(v1UpdateScheduleResponse);

  // Query maintenance status.
  v1::master::Call v1GetStatusCall;
  v1GetStatusCall.set_type(v1::master::Call::GET_MAINTENANCE_STATUS);

  Future<v1::master::Response> v1GetStatusResponse =
    post(master.get()->pid, v1GetStatusCall, contentType);

  AWAIT_READY(v1GetStatusResponse);
  ASSERT_TRUE(v1GetStatusResponse.get().IsInitialized());
  ASSERT_EQ(
      v1::master::Response::GET_MAINTENANCE_STATUS,
      v1GetStatusResponse.get().type());

  // Verify maintenance status matches the expectation.
  v1::maintenance::ClusterStatus status =
    v1GetStatusResponse.get().get_maintenance_status().status();
  ASSERT_EQ(2, status.draining_machines().size());
  ASSERT_EQ(0, status.down_machines().size());
}


class AgentAPITest
  : public MesosTest,
    public WithParamInterface<ContentType>
{
public:
  // Helper function to post a request to "/api/v1" agent endpoint and return
  // the response.
  Future<v1::agent::Response> post(
      const process::PID<slave::Slave>& pid,
      const v1::agent::Call& call,
      const ContentType& contentType)
  {
    process::http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    headers["Accept"] = stringify(contentType);

    return process::http::post(
        pid,
        "api/v1",
        headers,
        serialize(contentType, call),
        stringify(contentType))
      .then([contentType](const Response& response)
            -> Future<v1::agent::Response> {
        if (response.status != OK().status) {
          return Failure("Unexpected response status " + response.status);
        }
        return deserialize<v1::agent::Response>(contentType, response.body);
      });
  }
};


// These tests are parameterized by the content type of the HTTP request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    AgentAPITest,
    ::testing::Values(ContentType::PROTOBUF, ContentType::JSON));


TEST_P(AgentAPITest, GetFlags)
{
  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Slave::__recover);

  StandaloneMasterDetector detector;
  Try<Owned<cluster::Slave>> slave = this->StartSlave(&detector);
  ASSERT_SOME(slave);

  // Wait until the agent has finished recovery.
  AWAIT_READY(__recover);

  v1::agent::Call v1Call;
  v1Call.set_type(v1::agent::Call::GET_FLAGS);

  ContentType contentType = GetParam();

  Future<v1::agent::Response> v1Response =
    post(slave.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::agent::Response::GET_FLAGS, v1Response.get().type());
}


TEST_P(AgentAPITest, GetHealth)
{
  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Slave::__recover);

  StandaloneMasterDetector detector;
  Try<Owned<cluster::Slave>> slave = this->StartSlave(&detector);
  ASSERT_SOME(slave);

  // Wait until the agent has finished recovery.
  AWAIT_READY(__recover);

  v1::agent::Call v1Call;
  v1Call.set_type(v1::agent::Call::GET_HEALTH);

  ContentType contentType = GetParam();

  Future<v1::agent::Response> v1Response =
    post(slave.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::agent::Response::GET_HEALTH, v1Response.get().type());
  ASSERT_TRUE(v1Response.get().get_health().healthy());
}


TEST_P(AgentAPITest, GetVersion)
{
  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Slave::__recover);

  StandaloneMasterDetector detector;
  Try<Owned<cluster::Slave>> slave = this->StartSlave(&detector);
  ASSERT_SOME(slave);

  // Wait until the agent has finished recovery.
  AWAIT_READY(__recover);

  v1::agent::Call v1Call;
  v1Call.set_type(v1::agent::Call::GET_VERSION);

  ContentType contentType = GetParam();

  Future<v1::agent::Response> v1Response =
    post(slave.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::agent::Response::GET_VERSION, v1Response.get().type());

  ASSERT_EQ(MESOS_VERSION,
            v1Response.get().get_version().version_info().version());
}


TEST_P(AgentAPITest, GetLoggingLevel)
{
  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Slave::__recover);

  StandaloneMasterDetector detector;
  Try<Owned<cluster::Slave>> slave = this->StartSlave(&detector);
  ASSERT_SOME(slave);

  // Wait until the agent has finished recovery.
  AWAIT_READY(__recover);

  v1::agent::Call v1Call;
  v1Call.set_type(v1::agent::Call::GET_LOGGING_LEVEL);

  ContentType contentType = GetParam();

  Future<v1::agent::Response> v1Response =
    post(slave.get()->pid, v1Call, contentType);

  AWAIT_READY(v1Response);
  ASSERT_TRUE(v1Response.get().IsInitialized());
  ASSERT_EQ(v1::agent::Response::GET_LOGGING_LEVEL, v1Response.get().type());
  ASSERT_LE(0, FLAGS_v);
  ASSERT_EQ(
      v1Response.get().get_logging_level().level(),
      static_cast<uint32_t>(FLAGS_v));
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
