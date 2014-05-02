package store.client.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import store.client.exception.RequestFailedException;

/**
 * Files utilities.
 */
public final class Directories {

    private static final Path HOME = Paths.get(System.getProperty("user.home"), ".store");
    private static Path workingDirectory = Paths.get(".").toAbsolutePath().normalize();

    private Directories() {
    }

    /**
     * @return The client home directory.
     */
    public static Path home() {
        return HOME;
    }

    /**
     * @return The current working directory.
     */
    public static Path workingDirectory() {
        return workingDirectory;
    }

    /**
     * Change current working directory.
     *
     * @param path New working directory.
     */
    public static void changeWorkingDirectory(Path path) {
        Path absolute = resolve(path);
        if (!Files.exists(absolute)) {
            throw new RequestFailedException("Does not exists");
        }
        if (!Files.isDirectory(absolute)) {
            throw new RequestFailedException("Not a directory");
        }
        workingDirectory = absolute;
    }

    /**
     * Change current working directory to the user home directory.
     */
    public static void changeToUserHome() {
        workingDirectory = Paths.get(System.getProperty("user.home"));
    }

    /**
     * Resolve supplied path against current working directory.
     *
     * @param path A path.
     * @return The resulting path.
     */
    public static Path resolve(Path path) {
        return path.isAbsolute() ? path : workingDirectory.resolve(path).normalize();
    }
}
