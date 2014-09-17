package store.server.task;

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
import static store.server.config.ServerConfig.TASKS_POOL_SIZE;

/**
 * Provides asynchronous tasks execution infrastructure.
 */
public class TaskManager {

    private static final String SUCCESS = " - Success";
    private static final String FAILURE = " - Failure";
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
    private final ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     */
    public TaskManager(Config config) {
        executor = newScheduledThreadPool(config.getInt(TASKS_POOL_SIZE), new ThreadFactory() {
            private final ThreadFactory defaultFactory = defaultThreadFactory();
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = defaultFactory.newThread(runnable);
                thread.setName("task-" + counter.incrementAndGet());
                return thread;
            }
        });
    }

    /**
     * Asynchronously execute the given task.
     *
     * @param description Task short description, intended for logging purposes.
     * @param task The task to executes
     */
    public void execute(final String description, final Runnable task) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                LOG.info(description);
                try {
                    task.run();
                    LOG.debug(description + SUCCESS);

                } catch (Exception e) {
                    LOG.error(description + FAILURE, e);
                }
            }
        });
    }

    /**
     * Schedules execution of supplied task at periodic interval. If any execution of the task encounters an exception,
     * subsequent executions are suppressed. Otherwise, the task will only terminate via cancellation or when this
     * service is closed. If any execution of this task takes longer than its period, then subsequent executions may
     * start late, but will not concurrently execute.
     *
     * @param interval The duration between successive executions.
     * @param unit The time unit of the interval parameter.
     * @param description Task short description, intended for logging purposes.
     * @param task The task to executes
     * @return A task handle.
     */
    public Task schedule(long interval, TimeUnit unit, String description, Runnable task) {
        return new Task(executor.scheduleAtFixedRate(new LoggedRunnable(description, task), 0, interval, unit));
    }

    /**
     * Stops this manager, gracefully cancelling all scheduled tasks before.
     */
    public void stop() {
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
                LOG.debug(message + SUCCESS);

            } catch (Exception e) {
                LOG.error(message + FAILURE, e);
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
