package store.server.volume;

/**
 * An asynchronous task to execute on tested volume. Allows outcome retrieval.
 */
abstract class Task implements Runnable {

    private Throwable error;
    private boolean completed;

    @Override
    public final void run() {
        try {
            execute();

        } catch (Throwable e) {
            synchronized (this) {
                error = e;
            }
        } finally {
            synchronized (this) {
                completed = true;
                notifyAll();
            }
        }
    }

    /**
     * Actual task to execute.
     */
    protected abstract void execute();

    /**
     * Waits if necessary for task to complete. Returns true if no error occured.
     *
     * @return True if task has succeed, false if an exception has been thrown.
     */
    public synchronized final boolean hasSucceed() {
        awaitCompletion();
        return error == null;
    }

    /**
     * Waits if necessary for task to complete. Returns true if an instance of supplied class was thrown during task
     * execution.
     *
     * @param expectedError Class of the expected error
     * @return True if task has failed and an exception of this class has been thrown.
     */
    public synchronized final boolean hasFailed(Class<? extends Throwable> expectedError) {
        awaitCompletion();
        return error.getClass().equals(expectedError);
    }

    private void awaitCompletion() {
        while (!completed) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
