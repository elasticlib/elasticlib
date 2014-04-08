package store.server.service;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import store.common.Mappable;
import static store.common.yaml.YamlReading.readAll;
import store.common.yaml.YamlWriting;
import store.server.exception.WriteException;

/**
 * Provides a persistant storage for mappables instances. Not thread safe !
 */
class StorageService {

    private static final String EXTENSION = ".yml";
    private final Path home;
    private final Map<Class<?>, List<?>> cache = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home Storage home directory.
     */
    public StorageService(Path home) {
        this.home = home;
    }

    /**
     * Load all instances of supplied class.
     *
     * @param <T> Type of instances.
     * @param clazz Type of instances.
     * @return The list of theses instances.
     */
    public <T extends Mappable> List<T> loadAll(Class<T> clazz) {
        if (!cache.containsKey(clazz)) {
            Path path = path(clazz);
            List<T> list;
            if (!Files.exists(path)) {
                list = emptyList();
            } else {
                list = readAll(load(path), clazz);
            }
            cache.put(clazz, list);
        }
        return loadFromCache(clazz);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <T> List<T> loadFromCache(Class<T> clazz) {
        List cached = cache.get(clazz);
        return new ArrayList<>(cached);
    }

    /**
     * Save all supplied instances.
     *
     * @param <T> Type of instances.
     * @param clazz Type of instances.
     * @param mappables The list of instances to save.
     */
    public <T extends Mappable> void saveAll(Class<T> clazz, List<T> mappables) {
        Path path = path(clazz);
        if (mappables.isEmpty()) {
            delete(path);

        } else {
            save(path, YamlWriting.writeAll(mappables));
        }
        cache.remove(clazz);
    }

    /**
     * Add supplied instance in storage.
     *
     * @param <T> Instances type.
     * @param clazz Instance type.
     * @param mappable The instance to add.
     * @return True if supplied instance was actually added.
     */
    public <T extends Mappable> boolean add(Class<T> clazz, T mappable) {
        List<T> list = loadAll(clazz);
        if (list.contains(mappable)) {
            return false;
        }
        list.add(mappable);
        saveAll(clazz, list);
        return true;
    }

    /**
     * Remove supplied instance from storage.
     *
     * @param <T> Instances type.
     * @param clazz Instance type.
     * @param mappable The instance to remove.
     * @return True if supplied instance was actually removed.
     */
    public <T extends Mappable> boolean remove(Class<T> clazz, T mappable) {
        List<T> list = loadAll(clazz);
        if (!list.contains(mappable)) {
            return false;
        }
        list.remove(mappable);
        saveAll(clazz, list);
        return true;
    }

    private Path path(Class<?> clazz) {
        String name = clazz.getSimpleName();
        return home.resolve(Character.toLowerCase(name.charAt(0)) + name.substring(1) + EXTENSION);
    }

    private String load(Path path) {
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8)) {

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[1024];
            int length = reader.read(buffer);
            while (length > 0) {
                builder.append(buffer, 0, length);
                length = reader.read(buffer);
            }
            return builder.toString();

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private void save(Path path, String string) {
        try (OutputStream outputStream = Files.newOutputStream(path);
                Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8)) {

            writer.write(string);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private void delete(Path path) {
        try {
            Files.deleteIfExists(path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }
}
