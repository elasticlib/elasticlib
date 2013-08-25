package store.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import store.ContentReader;
import store.Store;
import store.hash.Digest;
import store.hash.Hash;
import store.info.ContentInfo;
import static store.info.ContentInfo.contentInfo;

public class Client {

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException();
        }
        switch (args[0]) {
            case "create":
                if (args.length < 2) {
                    throw new IllegalArgumentException();
                }
                Store.create(Paths.get(args[1]));
                return;

            case "put":
                if (args.length < 3) {
                    throw new IllegalArgumentException();
                }
                put(args[1], args[2]);
                return;

            case "get":
                if (args.length < 3) {
                    throw new IllegalArgumentException();
                }
                get(args[1], args[2]);
                return;


            case "delete":
                if (args.length < 3) {
                    throw new IllegalArgumentException();
                }
                delete(args[1], args[2]);
                return;

            default:
                throw new IllegalArgumentException(args[0]);
        }
    }

    private static void put(String storepath, String filepath) {
        Store store = Store.open(Paths.get(storepath));
        Path file = Paths.get(filepath);
        Digest digest = digest(file);
        ContentInfo contentInfo = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .build();

        try (InputStream inputStream = Files.newInputStream(file)) {
            store.put(contentInfo, inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(contentInfo.getHash());
    }

    private static Digest digest(Path path) {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return Digest.of(inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void get(String storepath, String encodedHash) {
        Store store = Store.open(Paths.get(storepath));
        Hash hash = new Hash(encodedHash);
        try (ContentReader reader = store.get(hash)) {
            ContentInfo contentInfo = reader.info();

            System.out.println("Hash : " + contentInfo.getHash());
            System.out.println("Length : " + contentInfo.getLength());

            Map<String, Object> metadata = contentInfo.getMetadata();
            if (metadata.isEmpty()) {
                System.out.println("No metadata");
            } else {
                System.out.println("Metadata : ");
                for (Entry<String, Object> entry : metadata.entrySet()) {
                    System.out.println(entry.getKey() + "=" + entry.getValue());
                }
            }
        }
    }

    private static void delete(String storepath, String encodedHash) {
        Store store = Store.open(Paths.get(storepath));
        Hash hash = new Hash(encodedHash);

        store.delete(hash);
    }
}
