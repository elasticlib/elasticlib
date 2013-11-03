package store.server;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import static java.nio.file.Files.walkFileTree;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Test utilities.
 */
public final class TestUtil {

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

    /**
     * Inject supplied value in field which fieldname is supplied. Field may be private and/or final.
     *
     * @param object Object in which value has to be injected.
     * @param fieldname Field name.
     * @param value Value to inject.
     */
    public static void inject(Object object, String fieldname, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldname);
            field.setAccessible(true);
            Field modifierField = Field.class.getDeclaredField("modifiers");
            modifierField.setAccessible(true);
            modifierField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(object, value);

        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
