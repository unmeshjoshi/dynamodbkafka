package dynamodbkafka.utils;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestUtils {

    public static void waitUntilTrue(Supplier<Boolean> condition, String msg,
                                     Duration waitTime) {
        try {
            var startTime = System.nanoTime();
            while (true) {
                if (condition.get())
                    return;

                if (System.nanoTime() > (startTime + waitTime.toNanos())) {
                    fail(msg);
                }

                Thread.sleep(100L);
            }
        } catch (InterruptedException e) {
            // should never hit here
            throw new RuntimeException(e);
        }
    }
}
