package store.server;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import static java.nio.file.Files.walkFileTree;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import store.common.Hash;

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
    public static final Content LOREM_IPSUM = new Content("loremIpsum.txt");

    private TestUtil() {
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
}
