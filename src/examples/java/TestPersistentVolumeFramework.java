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

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * A example to show how use persistent volume in java.
 * You could find more details about persistent volume in
 * <a href="https://issues.apache.org/jira/browse/MESOS-1554">MESOS-1554</a> .
 */
public class TestPersistentVolumeFramework {

  /**
   * A helper class that provide some methods to operate
   * {@link org.apache.mesos.Protos.Resource} .
   * Most logic converted from ./src/common/resources.cpp .
   */
  public static class Resources {

    /**
     * Tests if the given Resource object is empty.
     *
     * @param resource
     * @return
     */
    public static boolean isEmpty(Resource resource) {
      if (resource.getType() == Value.Type.SCALAR) {
        return resource.getScalar().getValue() == 0;
      }

      // Only consider scalar here because this example only use scalar type.
      return false;
    }

    /**
     * Validates the given Resource object. Returns Error if it is not
     * valid. A Resource object is valid if it has a name, a valid type,
     * i.e. scalar, range, or set, has the appropriate value set, and
     * a valid (role, reservation) pair for dynamic reservation.
     *
     * @param resource
     * @return
     */
    public static String validate(Resource resource) {
      if (resource.getName().isEmpty()) {
        return "Empty resource name";
      }

      // Only consider scalar here because this example only use scalar type.
      if (resource.getType() == Value.Type.SCALAR) {
        if (!resource.hasScalar() ||
            resource.hasRanges() ||
            resource.hasSet()) {
          return "Invalid scalar resource";
        }

        if (resource.getScalar().getValue() < 0) {
          return "Invalid scalar resource: value < 0";
        }
      } else {
        // Resource doesn't support TEXT or other value types.
        return "Unsupported resource type";
      }

      // Checks for 'disk' resource.
      if (resource.hasDisk() && !"disk".equals(resource.getName())) {
        return String.format(
            "DiskInfo should not be set for %s resource", resource.getName());
      }

      // Checks for the invalid state of (role, reservation) pair.
      if ("*".equals(resource.getRole()) && resource.hasReservation()) {
        return "Invalid reservation: role \"*\" cannot be dynamically reserved";
      }

      return "";
    }

    /**
     * Validates the given resources.
     *
     * @param resources
     * @return
     */
    public static String validate(List<Resource> resources) {
      for (Resource resource : resources) {
        String errMsg = validate(resource);
        if (!"".equals(errMsg)) {
          return String.format(
              "Resource '%s' is invalid: %s", resource, errMsg);
        }
      }

      return "";
    }

    /**
     * Tests if we can add two Resource objects together resulting in one
     * valid Resource object. For example, two Resource objects with
     * different name, type or role are not addable.
     *
     * @param left
     * @param right
     * @return
     */
    public static boolean addable(Resource left, Resource right) {
      if (!left.getName().equals(right.getName()) ||
          !left.getType().equals(right.getType()) ||
          !left.getRole().equals(right.getRole())) {
        return false;
      }

      // Check ReservationInfo.
      if (left.hasReservation() != right.hasReservation()) {
        return false;
      }

      if (left.hasReservation() &&
          left.getReservation().equals(right.getReservation())) {
        return false;
      }

      // Check DiskInfo.
      if (left.hasDisk() != right.hasDisk()) {
        return false;
      }

      if (left.hasDisk() && !left.getDisk().equals(right.getDisk())) {
        return false;
      }

      if (left.hasDisk() && left.getDisk().hasPersistence()) {
        return false;
      }

      // Check RevocableInfo.
      if (left.hasReservation() != right.hasReservation()) {
        return false;
      }

      return true;
    }
    /**
     * Tests if we can subtract "right" from "left" resulting in one valid
     * Resource object. For example, two Resource objects with different
     * name, type or role are not subtractable.
     * NOTE: Set subtraction is always well defined, it does not require
     * 'right' to be contained within 'left'. For example, assuming that
     * "left = {1, 2}" and "right = {2, 3}", "left" and "right" are
     * subtractable because "left - right = {1}". However, "left" does not
     * contain "right".
     *
     * @param left
     * @param right
     * @return
     */
    public static boolean subtractable(Resource left, Resource right) {
      if (!left.getName().equals(right.getName()) ||
          !left.getType().equals(right.getType()) ||
          !left.getRole().equals(right.getRole())) {
        return false;
      }

      // Check ReservationInfo.
      if (left.hasReservation() != right.hasReservation()) {
        return false;
      }

      if (left.hasReservation() &&
          left.getReservation().equals(right.getReservation())) {
        return false;
      }

      // Check DiskInfo.
      if (left.hasDisk() != right.hasDisk()) {
        return false;
      }

      if (left.hasDisk() && !left.getDisk().equals(right.getDisk())) {
        return false;
      }

      // NOTE: For Resource objects that have DiskInfo, we can only do
      // subtraction if they are equal.
      if (left.hasDisk() &&
          left.getDisk().hasPersistence() &&
          !left.equals(right)) {
        return false;
      }

      return true;
    }

    /**
     * Add up left and right, return a new value.
     *
     * @param left
     * @param right
     * @return
     */
    public static Resource add(Resource left, Resource right) {
      if (left.getType() == Value.Type.SCALAR) {
        double scalaValue = left.getScalar().getValue() +
                            right.getScalar().getValue();
        Scalar scalar = Scalar.newBuilder()
                              .setValue(scalaValue)
                              .build();
        Resource resource = Resource.newBuilder()
                                    .mergeFrom(left)
                                    .setScalar(scalar)
                                    .build();
        return resource;
      }

      // Only consider scalar here because this example only use scalar type.
      return left;
    }

    /**
     * Add resource to a exist resource list.
     *
     * @param resources
     * @param resource
     */
    public static void add(List<Resource> resources, Resource resource) {
      if (!validate(resource).isEmpty() || isEmpty(resource)) {
        return;
      }

      for (int i = 0; i < resources.size(); i++) {
        Resource tmpResource = resources.get(i);
        if (addable(tmpResource, resource)) {
          resource = add(tmpResource, resource);
          resources.set(i, resource);
          return;
        }
      }

      resources.add(resource);
    }

    /**
     * Remove right from left, return a new value.
     *
     * @param left
     * @param right
     * @return
     */
    public static Resource remove(Resource left, Resource right) {
      if (left.getType() == Value.Type.SCALAR) {
        double scalaValue = left.getScalar().getValue() -
                            right.getScalar().getValue();
        Scalar scalar = Scalar.newBuilder()
                              .setValue(scalaValue)
                              .build();
        Resource resource = Resource.newBuilder()
                                    .mergeFrom(left)
                                    .setScalar(scalar)
                                    .build();
        return resource;
      }

      // Only consider scalar here because this example only use scalar type.
      return left;
    }

    /**
     * Remove resource from a exist resource list.
     *
     * @param resources
     * @param resource
     */
    public static void remove(List<Resource> resources, Resource resource) {
      if (!validate(resource).isEmpty() || isEmpty(resource)) {
        return;
      }

      for (int i = 0; i < resources.size(); i++) {
        Resource tmpResource = resources.get(i);
        if (subtractable(tmpResource, resource)) {
          resource = remove(tmpResource, resource);
          if (!validate(resource).isEmpty() || isEmpty(resource)) {
            resources.remove(i);
            return;
          } else {
            resources.set(i, resource);
            return;
          }
        }
      }
    }

    /**
     * Checks if the left Resources is a superset of the right Resources.
     * @param left
     * @param right
     * @return
     */
    public static boolean contains(List<Resource> left, List<Resource> right) {
      for (Resource resource : right) {
        if (!contains(left, resource)) {
          return false;
        }

        remove(left, resource);
      }

      return true;
    }

    /**
     * Checks if the left Resources contains the right Resource.
     * @param right
     * @param left
     * @return
     */
    public static boolean contains(List<Resource> left, Resource right) {
      // NOTE: We must validate 'that' because invalid resources can lead
      // to false positives here (e.g., "cpus:-1" will return true). This
      // is because contains assumes resources are valid.
      if (!validate(right).isEmpty()) {
        return false;
      }

      for (Resource resource : left) {
        if (contains(resource, right)) {
          return true;
        }
      }

      return false;
    }

    /**
     * Checks if the left Resource contains the right Resource.
     * @param right
     * @param left
     * @return
     */
    public static boolean contains(Resource left, Resource right) {
      if (!subtractable(left, right)) {
        return false;
      }

      if (left.getType() == Value.Type.SCALAR) {
        return right.getScalar().getValue() <= left.getScalar().getValue();
      } else {
        // Not support other types because only use scalar type in this example.
        return false;
      }
    }

    /**
     * Certain offer operations (e.g., RESERVE, UNRESERVE, CREATE or
     * DESTROY) alter the offered resources. The following methods
     * provide a convenient way to get the transformed resources by
     * applying the given offer operation(s). Returns an Error message
     * if the offer operation(s) cannot be applied.
     *
     * @param resources
     * @param operation
     * @return
     */
    public static String apply(List<Resource> resources, Operation operation) {
      switch (operation.getType()) {
      case LAUNCH:
        break;

      case RESERVE:

      case UNRESERVE:
        // Unimplemented here because this operation type not used below.
        return "Unimplemented";

      case CREATE: {
          String errMsg = validate(operation.getCreate().getVolumesList());
          if (!"".equals(errMsg)) {
            return String.format("Invalid CREATE Operation: %s", errMsg);
          }

          for (Resource volume : operation.getCreate().getVolumesList()) {
            if (!volume.hasDisk()) {
              return "Invalid CREATE Operation: Missing 'disk'";
            } else if (!volume.getDisk().hasPersistence()) {
              return "Invalid CREATE Operation: Missing 'persistence'";
            }

            // Strip the disk info so that we can subtract it from the
            // original resources.
            Resource stripped = Resource.newBuilder()
                                        .mergeFrom(volume)
                                        .clearDisk()
                                        .build();

            if (!contains(resources, stripped)) {
              return "Invalid CREATE Operation: Insufficient disk resources";
            }

            remove(resources, stripped);
            add(resources, volume);
          }

          break;
        }
      case DESTROY: {
        String errMsg = validate(operation.getDestroy().getVolumesList());
        if (!"".equals(errMsg)) {
          return String.format("Invalid DESTROY Operation: %s", errMsg);
        }

        for (Resource volume : operation.getDestroy().getVolumesList()) {
          if (!volume.hasDisk()) {
            return "Invalid DESTROY Operation: Missing 'disk'";
          } else if (!volume.getDisk().hasPersistence()) {
            return "Invalid DESTROY Operation: Missing 'persistence'";
          }

          if (!contains(resources, volume)) {
            return
                "Invalid DESTROY Operation: Persistent volume does not exist";
          }

          Resource stripped = Resource.newBuilder()
                                      .mergeFrom(volume)
                                      .clearDisk()
                                      .build();
          remove(resources, volume);
          add(resources, stripped);
        }

        break;
      }
      default:
        return String.format("Unknown offer operation %s", operation.getType());
      }

      return "";
    }

    /**
     * Certain offer operations (e.g., RESERVE, UNRESERVE, CREATE or
     * DESTROY) alter the offered resources. The following methods
     * provide a convenient way to get the transformed resources by
     * applying the given offer operation(s). Returns an Error message
     * if the offer operation(s) cannot be applied.
     *
     * @param resources
     * @param operations
     * @return
     */
    public static String apply(
        List<Resource> resources,
        List<Operation> operations) {

      for (Operation operation : operations) {
        String errMsg = apply(resources, operation);
        if (!"".equals(errMsg)) {
          return errMsg;
        }
      }

      return "";
    }

  }

  /**
   * Helper method to create initial resources.
   *
   * @param role
   * @return
   */
  public static List<Resource> getShardInitialResources(String role) {
    double cpus = 0.1;
    int mem = 32;
    int disk = 16;
    List<Resource> resources = new ArrayList<Resource>();
    resources.add(Resource.newBuilder()
                          .setName("cpus")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder()
                                                 .setValue(cpus)
                                                 .build())
                          .setRole(role)
                          .build());
    resources.add(Resource.newBuilder()
                          .setName("mem")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder()
                                                 .setValue(mem)
                                                 .build())
                          .setRole(role)
                          .build());
    resources.add(Resource.newBuilder()
                          .setName("disk")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder()
                                                 .setValue(disk)
                                                 .build())
                          .setRole(role)
                          .build());
    return resources;
  }

  /**
   * Helper method to create a shard persistent volume.
   *
   * @param role
   * @param persistenceId
   * @param containerPath
   * @return
   */
  public static Resource getShardPersistentVolume(
      String role,
      String persistenceId,
      String containerPath) {
    Volume volume = Volume.newBuilder()
                          .setContainerPath(containerPath)
                          .setMode(Volume.Mode.RW)
                          .build();

    DiskInfo info = DiskInfo.newBuilder()
                            .setPersistence(Persistence.newBuilder()
                                                       .setId(persistenceId)
                                                       .build())
                            .setVolume(volume)
                            .build();

    Resource resource = Resource.newBuilder()
                                .setName("disk")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder()
                                                       .setValue(8)
                                                       .build())
                                .setRole(role)
                                .setDisk(info)
                                .build();
    return resource;
  }

  /**
   * Create a Create operation by a volume.
   *
   * @param volume
   * @return
   */
  public static Offer.Operation getCreateOperation(Resource volume) {
    Operation operation = Operation.newBuilder()
                                   .setType(Operation.Type.CREATE)
                                   .setCreate(Operation.Create
                                                       .newBuilder()
                                                       .addVolumes(volume))
                                   .build();
    return operation;
  }

  /**
   * Create a LAUNCH operation by a task.
   *
   * @param task
   * @return
   */
  public static Offer.Operation getLaunchOperation(TaskInfo task) {
    Operation operation = Operation.newBuilder()
                                   .setType(Operation.Type.LAUNCH)
                                   .setLaunch(Operation.Launch
                                                       .newBuilder()
                                                       .addTaskInfos(task))
                                   .build();
    return operation;
  }

  /**
   * Scheduler class
   */
  static class TestPersistentVolumeScheduler implements Scheduler {
    public TestPersistentVolumeScheduler(
        FrameworkInfo frameworkInfo,
        int numShards,
        int tasksPerShard) {
      this.frameworkInfo = frameworkInfo;
      for (int i = 0; i < numShards; i++) {
        shards.add(new Shard(
            "shard-" + i,
            frameworkInfo.getRole(),
            tasksPerShard));
      }
    }

    @Override
    public void registered(
        SchedulerDriver driver,
        FrameworkID frameworkId,
        MasterInfo masterInfo) {
      System.out.printf("Registered with master %s  and got framework ID %s\n",
                        masterInfo,
                        frameworkId);
      frameworkInfo = FrameworkInfo.newBuilder()
                                   .mergeFrom(frameworkInfo)
                                   .setId(frameworkId)
                                   .build();
    }

    @Override
    public void reregistered(SchedulerDriver driver,
                           MasterInfo masterInfo) {
      System.out.printf("Reregistered with master %s\n",
                        masterInfo);
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
      System.out.println("Disconnected!");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {
      for (Offer offer : offers) {
        System.out.printf("Received offer %s from slave %s (%s) with %s\n",
                          offer.getId(),
                          offer.getSlaveId(),
                          offer.getHostname(),
                          offer.getResourcesList());

        List<Resource> offered = new ArrayList<Resource>();
        offered.addAll(offer.getResourcesList());

        // The operation we will perform on the offer.
        List<Operation> operations = new ArrayList<Operation>();

        for (Shard shard : shards) {
            switch (shard.state) {
              case INIT:
                if (Resources.contains(offered, shard.resources)) {
                  Resource volume = getShardPersistentVolume(
                      frameworkInfo.getRole(),
                      UUID.randomUUID().toString(),
                      "volume");
                  String errMsg = Resources.apply(shard.resources,
                                                  getCreateOperation(volume));
                  if (!"".equals(errMsg)) {
                    System.out.printf("Error: %s\n", errMsg);
                    return;
                  }
                  List<Resource> resources = shard.resources;

                  TaskID taskId = TaskID.newBuilder()
                                        .setValue(UUID.randomUUID().toString())
                                        .build();
                  String strCommand = "touch volume/persisted";
                  CommandInfo commandInfo = CommandInfo.newBuilder()
                                                       .setValue(strCommand)
                                                       .build();
                  TaskInfo task = TaskInfo.newBuilder()
                                          .setName(shard.name)
                                          .setTaskId(taskId)
                                          .setSlaveId(offer.getSlaveId())
                                          .addAllResources(resources)
                                          .setCommand(commandInfo)
                                          .build();

                  // Update the shard.
                  shard.state = Shard.State.STAGING;
                  shard.taskId = task.getTaskId();
                  shard.volume.id = volume.getDisk().getPersistence().getId();
                  shard.volume.slave = offer.getSlaveId().getValue();
                  shard.resources = resources;
                  shard.launched++;

                  operations.add(getCreateOperation(volume));
                  operations.add(getLaunchOperation(task));

                  List<Operation> tmpOperations = new ArrayList<Operation>();
                  tmpOperations.add(getCreateOperation(volume));
                  tmpOperations.add(getLaunchOperation(task));
                  errMsg = Resources.apply(offered, tmpOperations);
                  resources = offered;

                  if (!"".equals(errMsg)) {
                    System.out.printf("Error: %s\n", errMsg);
                    return;
                  }
                  offered = resources;
                }
                break;
              case WAITING:
                if (Resources.contains(offered, shard.resources)) {
                  String slaveId = offer.getSlaveId().getValue();
                  assert(shard.volume.slave.equals(slaveId));

                  TaskID taskId = TaskID.newBuilder()
                                        .setValue(UUID.randomUUID().toString())
                                        .build();
                  String strCommand = "test -f volume/persisted";
                  CommandInfo commandInfo = CommandInfo.newBuilder()
                                                       .setValue(strCommand)
                                                       .build();
                  TaskInfo task = TaskInfo.newBuilder()
                                          .setName(shard.name)
                                          .setTaskId(taskId)
                                          .setSlaveId(offer.getSlaveId())
                                          .addAllResources(shard.resources)
                                          .setCommand(commandInfo)
                                          .build();

                  // Update the shard.
                  shard.state = Shard.State.STAGING;
                  shard.taskId = task.getTaskId();
                  shard.launched++;

                  operations.add(getLaunchOperation(task));
                }
                break;
              case STAGING:
              case RUNNING:
              case DONE:
                // Ignore the offer.
                break;
              default:
                System.out.printf("Unexpected shard state: %s\n", shard.state);
                driver.abort();
                break;
            }
        }

        List<OfferID> offerIds = new ArrayList<OfferID>();
        offerIds.add(offer.getId());
        Filters filters = Filters.newBuilder()
                                 .build();
        for (Operation operation : operations) {
          System.out.printf("Operation: %s\n", operation);
        }
        driver.acceptOffers(offerIds, operations, filters);
      }
    }

    @Override
    public void offerRescinded(
        SchedulerDriver driver,
        OfferID offerId) {
        System.out.printf("Offer %s has been rescinded\n", offerId);
    }

    @Override
    public void statusUpdate(
        SchedulerDriver driver,
        TaskStatus status) {
      System.out.printf("Task '%s' is in state %s",
                        status.getTaskId(),
                        status.getState());

      for (Shard shard : shards) {
        if (shard.taskId.equals(status.getTaskId())) {
          switch (status.getState()) {
            case TASK_RUNNING:
              shard.state = Shard.State.RUNNING;
              break;
            case TASK_FINISHED:
              if (shard.launched >= shard.tasks) {
                shard.state = Shard.State.DONE;
              } else {
                shard.state = Shard.State.WAITING;
              }
              break;
            case TASK_STAGING:
            case TASK_STARTING:
              // Ignore the status update.
              break;
            default:
              System.out.printf("Unexpected task state %s for task '%s'\n",
                                status.getState(),
                                status.getTaskId());
              driver.abort();
              break;
          }

          break;
        }
      }

      // Check the terminal condition.
      boolean terminal = true;
      for (Shard shard : shards) {
        if (shard.state != Shard.State.DONE) {
          terminal = false;
          break;
        }
      }

      if (terminal) {
        driver.stop();
      }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorId,
                                 SlaveID slaveId,
                                 byte[] data) {
      System.out.printf(
          "Received framework message from executor '%s' on slave %s: '%s'\n",
          executorId,
          slaveId,
          new String(data));
    }

    @Override
    public void slaveLost(
        SchedulerDriver driver,
        SlaveID slaveId) {
      System.out.printf("Lost slave %s\n", slaveId);
    }

    @Override
    public void executorLost(
        SchedulerDriver driver,
        ExecutorID executorId,
        SlaveID slaveId,
        int status) {
      System.out.printf("Lost executor '%s' on slave %s, status %s\n",
                        executorId,
                        slaveId,
                        status);
    }

    public void error(
        SchedulerDriver driver,
        String message) {
      System.out.printf("Error: %s\n", message);
    }

    static class Shard {
      enum State {
        INIT,       // The shard hasn't been launched yet.
        STAGING,    // The shard has been launched.
        RUNNING,    // The shard is running.
        WAITING,    // The shard is waiting to be re-launched.
        DONE,       // The shard has finished all tasks.
      }

      // The persistent volume associated with this shard.
      static class Volume {
        // The persistence ID.
        String id;

        // An identifier used to uniquely identify a slave (even across
        // reboot). In the test, we use the slave ID since slaves will not
        // be rebooted. Note that we cannot use hostname as the identifier
        // in a local cluster because all slaves share the same hostname.
        String slave;
      }

      public Shard(String name, String role, int tasks) {
        this.name = name;
        this.state = State.INIT;
        this.resources = getShardInitialResources(role);
        this.launched = 0;
        this.tasks = tasks;
      }

      String name;
      // The current state of this shard.
      State state;
      // The ID of the current task.
      TaskID taskId;
      // The persistent volume associated with the shard.
      Volume volume = new Volume();
      // Resources required to launch the shard.
      List<Resource> resources = new ArrayList<Resource>();
      // How many tasks this shard has launched.
      int launched;
      // How many tasks this shard should launch.
      int tasks;
    }

    FrameworkInfo frameworkInfo;
    List<Shard> shards = new ArrayList<Shard>();
  }

  /**
   * A simple java implement of cpp Flags class.
   * Used for handle command line args.
   */
  public static class Flags {

    private String  master;
    private String  role;
    private String  principal;
    private Integer numShards;
    private Integer tasksPerShard;
    private HashMap<String, String> helpMap  = new HashMap<String, String>();
    private HashMap<String, String> valueMap = new HashMap<String, String>();
    private String[] args;

    @SuppressWarnings("unchecked")
    private <T> T add(String name, String help, T defaultValue) {
      helpMap.put(name, help);
      if (valueMap.containsKey(name)) {
        String valueStr = valueMap.get(name);
        if (defaultValue instanceof String) {
          return (T) valueStr;
        } else if (defaultValue instanceof Integer) {
          return (T) new Integer(valueStr);
        } else {
          // Don't support other types here.
        }
      }
      return defaultValue;
    }

    /**
     * Returns a string describing the flags, preceded by a "usage
     * message" that will be prepended to that description.
     *
     * The 'message' passed to this function will be prepended
     * to the generated string returned from this function.
     *
     * The 'message' would be emitted first, followed by a blank line,
     * then followed by the flags description, for example:
     *
     *    Missing required --foo flag
     *
     *    Supported options:
     *
     *      --foo=VALUE       Description about 'bar' here. (default: 42)
     *
     * @param errMsg
     * @return
     */
    public String usage(String errMsg) {
      int padding = 5;
      StringBuilder sb = new StringBuilder();

      if (!errMsg.isEmpty()) {
        sb.append(errMsg);
        sb.append("\n\n");
      }

      sb.append(
          String.format("Usage: test-persistent-volume-framework [...]\n"));
      sb.append("Supported options:\n");

      HashMap<String, String> col1 = new HashMap<String, String>();
      int width = 0;
      for (String name : helpMap.keySet()) {
        col1.put(name, String.format("  --%s=VALUE", name));
        width = Math.max(width, col1.get(name).length());
      }

      for (String name : helpMap.keySet()) {
        String line = col1.get(name);
        String help = helpMap.get(name);

        int pos1 = 0, pos2 = 0;
        while (0 <= pos2 && pos2 < help.length()) {
          if (pos2 != 0) {
            sb.append(help.substring(pos1, pos2));
            sb.append('\n');
            pos1 = pos2 + 1;
            line = "";
          }

          sb.append(line);
          for (int i = 0, l = padding + width - line.length(); i < l; i++) {
            sb.append(' ');
          }
          pos2 = help.indexOf('\n', pos1);
        }

        sb.append(help.substring(pos1));
        sb.append('\n');
      }

      return sb.toString();
    }

    public String getMaster() {
      return master;
    }

    public String getRole() {
      return role;
    }

    public String getPrincipal() {
      return principal;
    }

    public Integer getNumShards() {
      return numShards;
    }

    public Integer getTasksPerShard() {
      return tasksPerShard;
    }

    /**
     * Load any flags from program execute params
     *
     * @param args execute params pass to the program
     *             when start it from command line
     * @return
     */
    public String load(String[] args) {
      this.args = args;
      for (String arg : args) {
        if (arg.startsWith("--")) {
          int pos = arg.indexOf('=');
          valueMap.put(arg.substring(2, pos), arg.substring(pos + 1));
        }
      }

      master = add("master",
                   "The master to connect to. May be one of:\n" +
                   "  master@addr:port (The PID of the master)\n" +
                   "  zk://host1:port1,host2:port2,.../path\n" +
                   "  zk://username:password@host1:port1,host2:port2,.../path\n" +
                   "  file://path/to/file (where file contains one of the above)",
                   "");
      role = add("role",
                 "Role to use when registering",
                 "test");
      principal = add("principal",
                      "The principal used to identify this framework",
                      "test");
      numShards = add("num_shards",
                      "The number of shards the framework will run.",
                      3);
      tasksPerShard = add("tasks_per_shard",
                          "The number of tasks should be launched per shard.",
                          3);

      return "";
    }
  }

  public static int EXIT_SUCCESS = 0;
  public static int EXIT_FAILURE = 1;

  public static void main(String[] args) throws Exception {
    Flags flags = new Flags();
    String errMsg = flags.load(args);
    if (!errMsg.isEmpty()) {
      System.err.println(flags.usage(errMsg));
      System.exit(EXIT_FAILURE);
    }

    if (flags.getMaster() == null || flags.getMaster().isEmpty()) {
      System.err.println(flags.usage("Missing required option --master"));
      System.exit(EXIT_FAILURE);
    }

    FrameworkInfo framework = FrameworkInfo.newBuilder()
                                           .setUser("")
                                           .setName(
                                               "Persistent Volume Framework (Java)")
                                           .setRole(flags.getRole())
                                           .setCheckpoint(true)
                                           .setPrincipal(flags.getPrincipal())
                                           .build();
    TestPersistentVolumeScheduler scheduler = new TestPersistentVolumeScheduler(
        framework,
        flags.getNumShards(),
        flags.getTasksPerShard());

    MesosSchedulerDriver driver = new MesosSchedulerDriver(
        scheduler,
        framework,
        flags.getMaster());

    int status = EXIT_SUCCESS;
    if (driver.run() != Status.DRIVER_STOPPED) {
      status = EXIT_FAILURE;
    }
    driver.stop();

    System.exit(status);
  }
}
