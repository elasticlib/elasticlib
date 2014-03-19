package store.server.volume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.common.Operation;
import store.server.CommandResult;
import store.server.Content;
import store.server.RevSpec;
import static store.server.TestUtil.recursiveDelete;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;

/**
 * Unit tests.
 */
public class VolumeTest {

    private static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
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

    private Volume newVolumeWith(Content content) {
        Volume volume = Volume.create(newTmpDir());
        try (InputStream inputStream = content.getInputStream()) {
            volume.put(content.getInfo(), inputStream, RevSpec.any());

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
        assertThat(hasContent(volume, LOREM_IPSUM.getHash())).isFalse();
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
            volume.put(LOREM_IPSUM.getInfo(), inputStream, RevSpec.any());
        }
        assertThat(hasContent(volume, LOREM_IPSUM.getHash())).isTrue();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(1);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test
    public void putInTwoSteps() throws IOException {
        Volume volume = Volume.create(newTmpDir());
        CommandResult firstStepResult = volume.put(LOREM_IPSUM.getInfo(), RevSpec.none());
        assertThat(firstStepResult.getOperation()).isEqualTo(Operation.CREATE);

        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            int transactionId = firstStepResult.getTransactionId();
            Hash hash = LOREM_IPSUM.getInfo().getHash();
            CommandResult secondStepResult = volume.create(transactionId, hash, inputStream);
            assertThat(secondStepResult).isEqualTo(firstStepResult);
        }
        assertThat(hasContent(volume, LOREM_IPSUM.getHash())).isTrue();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(1);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test
    public void putAlreadyStored() throws IOException {
        Volume volume = newVolumeWith(LOREM_IPSUM);
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            volume.put(LOREM_IPSUM.getInfo(), inputStream, RevSpec.any());
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
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = volume.get(LOREM_IPSUM.getHash())) {
            copy(inputStream, outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(expectedExceptions = UnknownContentException.class)
    public void getUnknownHash() throws IOException {
        Volume.create(newTmpDir()).get(LOREM_IPSUM.getHash());
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
    @Test(expectedExceptions = UnknownContentException.class)
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
        volume.delete(LOREM_IPSUM.getHash(), RevSpec.any());
        assertThat(hasContent(volume, LOREM_IPSUM.getHash())).isFalse();
        assertThat(volume.history(true, 0, Integer.MAX_VALUE)).hasSize(2);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownContentException.class)
    public void deleteUnknownHash() {
        Volume.create(newTmpDir()).delete(UNKNOWN_HASH, RevSpec.any());
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = RepositoryNotStartedException.class)
    public void stop() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.info(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test
    public void stopAndStart() {
        Volume volume = Volume.create(newTmpDir());
        volume.stop();
        volume.start();
        assertThat(hasContent(volume, UNKNOWN_HASH)).isFalse();
    }

    private static boolean hasContent(Volume volume, Hash hash) {
        try {
            InputStream inputStream = volume.get(hash);
            try {
                inputStream.close();
            } catch (IOException e) {
                // Impossible
                throw new AssertionError(e);
            }
        } catch (UnknownContentException e) {
            return false;
        }
        return true;
    }
}
