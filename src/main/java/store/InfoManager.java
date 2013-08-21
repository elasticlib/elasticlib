package store;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import static store.ContentInfo.contentInfo;
import store.hash.Hash;
import store.io.ObjectDecoder;
import static store.io.ObjectEncoder.encoder;
import store.io.StreamDecoder;

public class InfoManager {

    private final Path root;
    private final Map<Hash, ContentInfo> info = new HashMap<>();

    public InfoManager(Path root) {
        this.root = root;
    }

    public void add(ContentInfo contentInfo) {
        byte[] bytes = encoder()
                .put("hash", contentInfo.getHash()
                .value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();

        try {
            Files.write(bucket(contentInfo.getHash()),
                        bytes,
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ContentInfo get(Hash hash) {
        if (!info.containsKey(hash)) {
            Path bucket = bucket(hash);
            if (!Files.exists(bucket)) {
                throw new IllegalArgumentException("Unknown hash");
            }
            try (StreamDecoder streamDecoder = streamDecoder(bucket)) {
                while (streamDecoder.hasNext()) {
                    info.put(hash, read(streamDecoder.next()));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        ContentInfo contentInfo = info.get(hash);
        if (contentInfo == null) {
            throw new IllegalArgumentException("Unknown hash");
        }
        return contentInfo;
    }

    private static StreamDecoder streamDecoder(Path path) throws IOException {
        return new StreamDecoder(new BufferedInputStream(Files.newInputStream(path)));
    }

    private static ContentInfo read(ObjectDecoder objectDecoder) {
        return contentInfo()
                .withHash(new Hash(objectDecoder.getRaw("hash")))
                .withLength(objectDecoder.getLong("length"))
                .withMetadata(objectDecoder.getMap("metadata"))
                .build();
    }

    private Path bucket(Hash hash) {
        return root.resolve(hash.encode()
                .substring(0, 2));
    }
}
