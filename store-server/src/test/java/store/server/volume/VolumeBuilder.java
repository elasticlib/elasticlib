package store.server.volume;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import store.common.Hash;
import store.server.Content;
import store.server.volume.Volume;

public class VolumeBuilder {

    private final Volume volume;

    private VolumeBuilder(Path path) {
        volume = Volume.create(path);
    }

    public static VolumeBuilder volume(Path path) {
        return new VolumeBuilder(path);
    }

    public VolumeBuilder put(Content content) {
        try (InputStream inputStream = content.getInputStream()) {
            volume.put(content.getInfo(), inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public VolumeBuilder delete(Content content) {
        return delete(content.getHash());
    }

    public VolumeBuilder delete(Hash hash) {
        volume.delete(hash);
        return this;
    }

    public Volume build() {
        return volume;
    }
}
