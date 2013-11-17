package store.server.volume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import store.server.exception.VolumeNotStartedException;

/**
 * Unit tests.
 */
public class VolumeTest {

    private static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
    private final Executor executor = Executors.newCachedThreadPool();
    private Path path;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(this.getClass().getName());
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
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

    private Volume newVolumeWith(Content content) {
        Volume volume = Volume.create(newTmpDir());
        try (InputStream inputStream = content.getInputStream()) {
            volume.put(content.getInfo(), inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return volume;
    }

    /**
     * Test.
     */
    @Test
    public void create() {
        Volume volume = Volume.create(newTmpDir());
        assertThat(volume.contains(LOREM_IPSUM.getHash())).isFalse();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(0);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test
    public void put() throws IOException {
        Volume volume = Volume.create(newTmpDir());
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            volume.put(LOREM_IPSUM.getInfo(), inputStream);
        }
        assertThat(volume.contains(LOREM_IPSUM.getHash())).isTrue();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(1);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(expectedExceptions = ContentAlreadyStoredException.class)
    public void putAlreadyStored() throws IOException {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            volume.put(LOREM_IPSUM.getInfo(), inputStream);
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test
    public void get() throws IOException {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            volume.get(LOREM_IPSUM.getHash(), outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(expectedExceptions = UnknownHashException.class)
    public void getUnknownHash() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Volume.create(newTmpDir()).get(UNKNOWN_HASH, outputStream);
        }
    }

    /**
     * Test.
     */
    @Test
    public void info() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        assertThat(volume.info(LOREM_IPSUM.getHash())).isEqualTo(LOREM_IPSUM.getInfo());
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownHashException.class)
    public void infoUnknownHash() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        volume.info(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test
    public void delete() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        volume.delete(LOREM_IPSUM.getHash());
        assertThat(volume.contains(LOREM_IPSUM.getHash())).isFalse();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(2);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownHashException.class)
    public void deleteUnknownHash() {
        Volume.create(newTmpDir()).delete(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
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

    /**
     * Test.
     */
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

    /**
     * Test.
     */
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

    /**
     * Test.
     */
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

    /**
     * Test.
     */
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

    /**
     * Test.
     */
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

    /**
     * Test.
     */
    @Test
    public void getAndDelete() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
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

    /**
     * Test.
     */
    @Test
    public void deleteAndGet() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
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

    /**
     * Test.
     */
    @Test
    public void getTwice() {
        Volume volume = newVolumeWith(LOREM_IPSUM);
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

    /**
     * Test.
     */
    @Test(expectedExceptions = VolumeNotStartedException.class)
    public void stop() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.contains(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test
    public void stopAndStart() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.start();
        assertThat(volume.contains(UNKNOWN_HASH)).isFalse();
    }

    /**
     * Test.
     */
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

        assertThat(first.hasFailed(VolumeNotStartedException.class)).isTrue();
        assertThat(second.hasFailed(VolumeNotStartedException.class)).isTrue();
    }
}
