package store.common;

import java.nio.file.Path;

public class Config {

    private final Path root;

    public Config(Path root) {
        this.root = root;
    }

    public Path getRoot() {
        return root;
    }
}
