package io.github.retz.chopsticks;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ChopsticksScheduler implements Scheduler {
    static final Logger LOG = LoggerFactory.getLogger(ChopsticksScheduler.class);

    Protos.TaskID taskID;

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        switch (status.getState().getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
            case Protos.TaskState.TASK_LOST_VALUE:
                LOG.info(status.getMessage());
                driver.stop();
                break;
            default:
                LOG.error(status.getMessage());
        }
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.info("{} resource offered", offers.size());
        if (offers.isEmpty()) {
            throw new AssertionError();
        }

        taskID = Protos.TaskID.newBuilder().setValue("chopsticks-task-foobar").build();
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setTaskId(taskID)
                .setCommand(Protos.CommandInfo.newBuilder()
                        .setShell(true)
                        .setValue("uname -a"))
                .setContainer(
                        Protos.ContainerInfo.newBuilder().setDocker(
                                Protos.ContainerInfo.DockerInfo.newBuilder()
                                        .setImage("ubuntu:latest")))
                .build();

        List<Protos.Offer.Operation> operations = new LinkedList<>();

        operations.add(Protos.Offer.Operation.newBuilder()
                .setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(Protos.Offer.Operation.Launch.newBuilder()
                        .addTaskInfos(task)
                        .build())
                .build());

        List<Protos.OfferID> offerIds = new ArrayList<>();
        offerIds.add(offers.get(0).getId());

        driver.acceptOffers(offerIds, operations, Protos.Filters.getDefaultInstance());
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOG.error(message);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {

    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {

    }

    @Override
    public void disconnected(SchedulerDriver driver) {

    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {

    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOG.info(new String(data));
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
    }
}
