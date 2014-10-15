package store.server;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import static java.nio.file.Files.walkFileTree;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import store.common.config.Config;
import store.common.hash.Hash;
import static store.server.config.ServerConfig.JE_LOCK_TIMEOUT;
import static store.server.config.ServerConfig.STAGING_SESSIONS_CLEANUP_ENABLED;
import static store.server.config.ServerConfig.STAGING_SESSIONS_CLEANUP_INTERVAL;
import static store.server.config.ServerConfig.STAGING_SESSIONS_MAX_SIZE;
import static store.server.config.ServerConfig.STAGING_SESSIONS_TIMEOUT;
import static store.server.config.ServerConfig.STORAGE_SYNC_ENABLED;
import static store.server.config.ServerConfig.STORAGE_SYNC_INTERVAL;
import static store.server.config.ServerConfig.TASKS_POOL_SIZE;

/**
 * Test utilities.
 */
public final class TestUtil {

    /**
     * An unknown hash.
     */
    public static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    /**
     * A test content.
     */
    public static final TestContent LOREM_IPSUM = TestContent.of("loremIpsum.txt", "text/plain");

    private TestUtil() {
    }

    /**
     * @return Test config.
     */
    public static Config config() {
        return new Config()
                .set(TASKS_POOL_SIZE, 1)
                .set(STAGING_SESSIONS_MAX_SIZE, 10)
                .set(STAGING_SESSIONS_TIMEOUT, "10 s")
                .set(STAGING_SESSIONS_CLEANUP_ENABLED, true)
                .set(STAGING_SESSIONS_CLEANUP_INTERVAL, "10 s")
                .set(STORAGE_SYNC_ENABLED, true)
                .set(STORAGE_SYNC_INTERVAL, "10 s")
                .set(JE_LOCK_TIMEOUT, "1 min");
    }

    /**
     * Recursively delete everything under (and including) supplied path.
     *
     * @param path Path of the file/directory to delete.
     * @throws IOException If an IO error occurs.
     */
    public static void recursiveDelete(Path path) throws IOException {
        walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;

                } else {
                    throw e;
                }
            }
        });
    }

    /**
     * Checks that supplied runnable will eventually succesfully run.
     *
     * @param runnable Runnable to execute.
     */
    public static void async(Runnable runnable) {
        int timeout = 60_000;
        int delay = 25;
        int time = 0;
        while (true) {
            try {
                wait(delay);
                time += delay;
                delay *= 4;
                if (delay > 5000) {
                    delay = 5000;
                }
                runnable.run();
                return;

            } catch (Exception e) {
                if (time >= timeout) {
                    throw new AssertionError(e);
                }
            } catch (AssertionError e) {
                if (time >= timeout) {
                    throw e;
                }
            } catch (Error e) {
                throw e;
            }
        }
    }

    private static void wait(int millis) {
        try {
            Thread.sleep(millis);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
