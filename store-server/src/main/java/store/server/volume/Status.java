package store.server.volume;

import java.nio.file.Path;

public final class Status {

    private final Path path;
    private final boolean started;

    Status(Path path, boolean started) {
        this.path = path;
        this.started = started;
    }

    public String getName() {
        return path.getFileName().toString();
    }

    public Path getPath() {
        return path;
    }

    public boolean isStarted() {
        return started;
    }
}
