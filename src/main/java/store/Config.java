package store;

import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;

public class Config {

    private final Path root;
    private final List<Path> volumePaths;

    public Config(Path root, List<Path> volumePaths) {
        this.root = root;
        this.volumePaths = unmodifiableList(new ArrayList<>(volumePaths));
    }

    public Path getRoot() {
        return root;
    }

    public List<Path> getVolumePaths() {
        return volumePaths;
    }
}
