package store.server.service;

import java.nio.file.Path;

/**
 * Represents the status of a repository.
 */
public final class Status {

    private final Path path;
    private final boolean started;

    Status(Path path, boolean started) {
        this.path = path;
        this.started = started;
    }

    /**
     * @return The repository name.
     */
    public String getName() {
        return path.getFileName().toString();
    }

    /**
     * @return The repository home path.
     */
    public Path getPath() {
        return path;
    }

    /**
     * @return True if repository is started.
     */
    public boolean isStarted() {
        return started;
    }
}
