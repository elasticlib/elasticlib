package store.server.volume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.Hash;
import store.server.Content;
import static store.server.TestUtil.inject;
import static store.server.TestUtil.recursiveDelete;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import static store.server.volume.VolumeBuilder.volume;

public class VolumeTest {

    private static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
    private final Executor executor = Executors.newCachedThreadPool();
    private Path path;

    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(this.getClass().getName());
    }

    @AfterClass
    public void cleanUp() throws IOException {
        recursiveDelete(path);
    }

    private Path newTmpDir() {
        try {
            return Files.createTempDirectory(path, "tmp");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BlockingTransactionManager injectTxManager(Volume volume) {
        BlockingTransactionManager txManager = new BlockingTransactionManager(newTmpDir());
        inject(volume, "transactionManager", txManager);
        return txManager;
    }

    @Test
    public void create() {
        Volume volume = Volume.create(newTmpDir());
        assertThat(volume.contains(LOREM_IPSUM.getHash())).isFalse();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(0);
    }

    @Test
    public void put() {
        Volume volume = volume(newTmpDir()).put(LOREM_IPSUM).build();
        assertThat(volume.contains(LOREM_IPSUM.getHash())).isTrue();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(1);
    }

    @Test(expectedExceptions = ContentAlreadyStoredException.class)
    public void putAlreadyStored() {
        volume(path.resolve("putAlreadyStored"))
                .put(LOREM_IPSUM)
                .put(LOREM_IPSUM);
    }

    @Test
    public void get() throws IOException {
        Volume volume = volume(newTmpDir()).put(LOREM_IPSUM).build();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            volume.get(LOREM_IPSUM.getHash(), outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    @Test(expectedExceptions = UnknownHashException.class)
    public void getUnknownHash() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Volume.create(newTmpDir()).get(UNKNOWN_HASH, outputStream);
        }
    }

    @Test
    public void info() {
        Volume volume = volume(newTmpDir()).put(LOREM_IPSUM).build();
        assertThat(volume.info(LOREM_IPSUM.getHash())).isEqualTo(LOREM_IPSUM.getInfo());
    }

    @Test(expectedExceptions = UnknownHashException.class)
    public void infoUnknownHash() {
        Volume.create(path.resolve("infoUnknownHash")).info(UNKNOWN_HASH);
    }

    @Test
    public void delete() {
        Volume volume = volume(newTmpDir())
                .put(LOREM_IPSUM)
                .delete(LOREM_IPSUM)
                .build();

        assertThat(volume.contains(LOREM_IPSUM.getHash())).isFalse();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(2);
    }

    @Test(expectedExceptions = UnknownHashException.class)
    public void deleteUnknownHash() {
        Volume.create(newTmpDir()).delete(UNKNOWN_HASH);
    }

    @Test
    public void putTwice() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutTask(volume, LOREM_IPSUM);
        Task second = new PutTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasFailed(ContentAlreadyStoredException.class)).isTrue();
    }

    @Test
    public void putTwiceWithFailure() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutWithFailureTask(volume, LOREM_IPSUM);
        Task second = new PutTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasFailed(IntegrityCheckingFailedException.class)).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test
    public void putAndDelete() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutTask(volume, LOREM_IPSUM);
        Task second = new DeleteTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test
    public void putAndDeleteWithFailure() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutWithFailureTask(volume, LOREM_IPSUM);
        Task second = new DeleteTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasFailed(IntegrityCheckingFailedException.class)).isTrue();
        assertThat(second.hasFailed(UnknownHashException.class)).isTrue();
    }

    @Test
    public void getAndPut() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new GetTask(volume, LOREM_IPSUM);
        Task second = new PutTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasFailed(UnknownHashException.class)).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test
    public void putAndGet() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutTask(volume, LOREM_IPSUM);
        Task second = new GetTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test
    public void getAndDelete() {
        Volume volume = volume(newTmpDir())
                .put(LOREM_IPSUM)
                .build();

        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new GetTask(volume, LOREM_IPSUM);
        Task second = new DeleteTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test
    public void deleteAndGet() {
        Volume volume = volume(newTmpDir())
                .put(LOREM_IPSUM)
                .build();

        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new DeleteTask(volume, LOREM_IPSUM);
        Task second = new GetTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasFailed(UnknownHashException.class)).isTrue();
    }

    @Test
    public void getTwice() {
        Volume volume = volume(newTmpDir())
                .put(LOREM_IPSUM)
                .build();

        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new GetTask(volume, LOREM_IPSUM);
        Task second = new GetTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        txManager.proceed();

        assertThat(first.hasSucceed()).isTrue();
        assertThat(second.hasSucceed()).isTrue();
    }

    @Test(expectedExceptions = VolumeClosedException.class)
    public void stop() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.contains(UNKNOWN_HASH);
    }

    @Test
    public void stopAndStart() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.start();
        assertThat(volume.contains(UNKNOWN_HASH)).isFalse();
    }

    @Test
    public void stopAndPut() {
        Volume volume = Volume.create(newTmpDir());
        BlockingTransactionManager txManager = injectTxManager(volume);
        Task first = new PutTask(volume, LOREM_IPSUM);
        Task second = new PutTask(volume, LOREM_IPSUM);

        executor.execute(first);
        txManager.awaitReady();
        executor.execute(second);
        volume.stop();
        txManager.proceed();

        assertThat(first.hasFailed(VolumeClosedException.class)).isTrue();
        assertThat(second.hasFailed(VolumeClosedException.class)).isTrue();
    }
}
