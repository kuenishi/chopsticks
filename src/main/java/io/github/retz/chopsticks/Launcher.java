package io.github.retz.chopsticks;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {
    static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

    public static void main(String ... args) {

        LOG.info("foobar");
        System.exit(exec(args));
    }
    private static int exec(String ... args) {
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("chopsticks")
                .setName("CHOPSTICKS")
                .setPrincipal("chopsticks")
                .setRole("chopsticks")
                .build();

        ChopsticksScheduler scheduler = new ChopsticksScheduler();

        String master = "192.168.201.239:5050";
        SchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkInfo, master);

        driver.start();
        return (driver.join() == Protos.Status.DRIVER_STOPPED)? 0 : 254;
    }
}
