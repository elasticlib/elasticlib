package store.server.volume;

public abstract class Task implements Runnable {

    private Throwable error;
    private boolean completed;

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

    protected abstract void execute();

    public final synchronized boolean hasSucceed() {
        awaitCompletion();
        return error == null;
    }

    public final synchronized boolean hasFailed(Class<? extends Throwable> expectedError) {
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
