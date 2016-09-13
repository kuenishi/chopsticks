package io.github.retz.chopsticks;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Launcher {
    static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

    public static void main(String... args) {

        System.exit(exec(args));
    }

    private static int exec(String... args) {
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("root")
                .setName("CHOPSTICKS")
                .setPrincipal("chopsticks")
                .setRole("chopsticks")
                .addCapabilities(Protos.FrameworkInfo.Capability.newBuilder().setType(Protos.FrameworkInfo.Capability.Type.GPU_RESOURCES))
                .build();

        LOG.info("Starting Chopstick scheduler with GPU_RESOURCES...");

        String[] command = {"nvidia-smi"};
        ChopsticksScheduler scheduler = new ChopsticksScheduler(Arrays.asList(command), "nvidia/cuda");

        //String master = "127.0.0.1:5050";
        String master = "192.168.100.128:5050";
        SchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkInfo, master);

        if (driver.start() == Protos.Status.DRIVER_RUNNING) {
            LOG.info("Driver started. Waiting for resource offers");
        } else {
            LOG.error("Driver couldn't start");
            return -1;
        }
        return (driver.join() == Protos.Status.DRIVER_STOPPED) ? 0 : 254;
    }
}
