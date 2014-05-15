package store.server.async;

import java.util.concurrent.Future;

/**
 * A handle on a periodic task.
 */
public class Task {

    private final Future<?> future;

    /**
     * Constructor.
     *
     * @param future Wrapped future.
     */
    Task(Future<?> future) {
        this.future = future;
    }

    /**
     * Cancel this tasks, letting current execution completes (if any).
     */
    public void cancel() {
        future.cancel(false);
    }
}
