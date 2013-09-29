package store.server.volume;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.ContentInfo;
import store.common.Hash;
import store.common.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;

public class Volume {

    private final Uid uid;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Volume(Uid uid, InfoManager infoManager, ContentManager contentManager) {
        this.uid = uid;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    public static Volume create(Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidStorePathException();
            }
            Uid uid = Uid.random();
            writeUid(path.resolve("uid"), uid);
            return new Volume(uid,
                              InfoManager.create(path.resolve("info")),
                              ContentManager.create(path.resolve("content")));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static Volume open(Path path) {
        return new Volume(readUid(path.resolve("uid")),
                          InfoManager.open(path.resolve("info")),
                          ContentManager.open(path.resolve("content")));
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    private static void writeUid(Path path, Uid uid) {
        try (OutputStream outputStream = Files.newOutputStream(path);
                Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8);
                BufferedWriter bufferedWriter = new BufferedWriter(writer)) {

            bufferedWriter.write(uid.toString());

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    private static Uid readUid(Path path) {
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {

            return new Uid(bufferedReader.readLine());

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public Uid getUid() {
        return uid;
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        contentManager.put(contentInfo, source);
        infoManager.put(contentInfo);
    }

    public void delete(Hash hash) {
        infoManager.delete(hash);
        contentManager.delete(hash);
    }

    public boolean contains(Hash hash) {
        return infoManager.contains(hash);
    }

    public Optional<ContentInfo> info(Hash hash) {
        return infoManager.get(hash);
    }

    public InputStream get(Hash hash) {
        return contentManager.get(hash);
    }
}
