package store.server.async;

import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import static store.server.config.ServerConfig.ASYNC_POOL_SIZE;

/**
 * Provides periodic tasks execution service.
 */
public class AsyncManager {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncManager.class);
    private final ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     */
    public AsyncManager(Config config) {
        executor = newScheduledThreadPool(config.getInt(ASYNC_POOL_SIZE), new ThreadFactory() {
            private final ThreadFactory defaultFactory = defaultThreadFactory();
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = defaultFactory.newThread(runnable);
                thread.setName("async-service-thread-" + counter.incrementAndGet());
                return thread;
            }
        });
    }

    /**
     * Schedules execution of supplied task at periodic interval. If any execution of the task encounters an exception,
     * subsequent executions are suppressed. Otherwise, the task will only terminate via cancellation or when this
     * service is closed. If any execution of this task takes longer than its period, then subsequent executions may
     * start late, but will not concurrently execute.
     *
     * @param period The period between successive executions.
     * @param unit The time unit of the period parameter.
     * @param description Task short description, intended for logging purposes.
     * @param command The task to executes
     * @return A task handle.
     */
    public Task schedule(long period, TimeUnit unit, String description, Runnable command) {
        return new Task(executor.scheduleAtFixedRate(new LoggedRunnable(description, command), 0, period, unit));
    }

    /**
     * Closes this service, gracefully cancelling all scheduled tasks before.
     */
    public void close() {
        executor.shutdown();
    }

    /**
     * Wraps a task execution, adding logging support.
     */
    private static class LoggedRunnable implements Runnable {

        // Task should never concurrently execute, but an atomic counter is nevertheless safer if this ever happens.
        private final AtomicLong counter = new AtomicLong();
        private final String description;
        private final Runnable delegate;

        public LoggedRunnable(String description, Runnable delegate) {
            this.description = description;
            this.delegate = delegate;
        }

        @Override
        public void run() {
            String message = format(description, counter.incrementAndGet());
            LOG.info(message);
            try {
                delegate.run();
                LOG.debug(message + " - Success");

            } catch (Exception e) {
                LOG.error(message + " - Failure", e);
                throw new AbortException(e);
            }
        }

        private static String format(String desc, long seq) {
            return "(#" + seq + ") " + desc;
        }
    }

    /**
     * Thrown if a task execution encounters an error, in order to cancel latter executions.
     */
    private static class AbortException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public AbortException(Throwable cause) {
            super(cause);
        }
    }
}