package io.github.retz.chopsticks;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ChopsticksScheduler implements Scheduler {
    static final Logger LOG = LoggerFactory.getLogger(ChopsticksScheduler.class);

    Protos.TaskID taskID;
    List<String> command;
    String image;

    public ChopsticksScheduler(List<String> command, String image) {
        LOG.info("Initialzing ChopstickScheduler command='{}'",
                String.join(" ", command));
        this.command = Objects.requireNonNull(command);
        this.image = Objects.requireNonNull(image);
        // TODO: check for command validity
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        switch (status.getState().getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
            case Protos.TaskState.TASK_LOST_VALUE:
                LOG.info(status.getMessage());
                System.err.println(status.getMessage());
                driver.stop();
                break;
            default:
                System.err.println(status.getMessage());
                LOG.error(status.getMessage());
        }
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.info("{} resource offered", offers.size());
        System.err.println(offers.size() + " resource offered");
        if (offers.isEmpty()) {
            throw new AssertionError();
        }

        Protos.Offer offer = offers.get(0);

        LOG.info(offer.toString());

        // GO SEE
        // https://github.com/apache/mesos/blob/master/docs/gpu-support.md#minimal-setup-with-support-for-docker-containers
        // https://github.com/apache/mesos/blob/master/docs/container-image.md
        LOG.info("Image: {}", image);
        taskID = Protos.TaskID.newBuilder().setValue("chopsticks-task-foobar").build();
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setTaskId(taskID)
                .setName("chopsticks-uname")
                .setSlaveId(offer.getSlaveId())
                .setCommand(Protos.CommandInfo.newBuilder()
                        .setShell(true)
                        .setValue(String.join(" ", command)))
                //.setContainer(getDocker("ubuntu:latest"))
                .setContainer(getMesos(image))
                .addAllResources(offer.getResourcesList())
                .build();

        List<Protos.Offer.Operation> operations = new LinkedList<>();

        operations.add(Protos.Offer.Operation.newBuilder()
                .setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(Protos.Offer.Operation.Launch.newBuilder()
                        .addTaskInfos(task)
                        .build())
                .build());

        List<Protos.OfferID> offerIds = new ArrayList<>();
        offerIds.add(offer.getId());

        LOG.info("running 1 task...");
        driver.acceptOffers(offerIds, operations, Protos.Filters.getDefaultInstance());
    }

    private Protos.ContainerInfo getDocker(String image) {
        return Protos.ContainerInfo.newBuilder().setDocker(
                Protos.ContainerInfo.DockerInfo.newBuilder()
                        .setImage(image))
                .setType(Protos.ContainerInfo.Type.DOCKER)
                .build();
    }

    private Protos.ContainerInfo getMesos(String image) {
        LOG.info("MesosInfo => DOCKER, {}", image);
        Protos.ContainerInfo ci = Protos.ContainerInfo.newBuilder()
                .setMesos(Protos.ContainerInfo.MesosInfo.newBuilder().setImage(
                        Protos.Image.newBuilder()
                                .setType(Protos.Image.Type.DOCKER)
                                .setDocker(Protos.Image.Docker.newBuilder().setName(image))
                ))
                .setType(Protos.ContainerInfo.Type.MESOS)
                .build();
        LOG.info(ci.toString());
        return ci;
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
        LOG.info("Framework registered as {}", frameworkId.getValue());
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
