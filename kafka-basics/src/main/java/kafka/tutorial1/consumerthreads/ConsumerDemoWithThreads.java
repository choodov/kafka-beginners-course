package kafka.tutorial1.consumerthreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    private static final String FIRST_TOPIC_NAME = "first_topic";
    private static final String DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "group_second";
    public static final String MESSAGE_APP_IS_INTERRUPTED = "App is interrupted";

    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(FIRST_TOPIC_NAME, DEFAULT_BOOTSTRAP_SERVER, GROUP_ID, latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Shutdown hook");
                    ((ConsumerRunnable) consumerRunnable).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("App has exited");
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(MESSAGE_APP_IS_INTERRUPTED, e);
        }

    }
}
