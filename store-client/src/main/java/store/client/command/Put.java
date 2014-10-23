package store.client.command;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.size;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import static store.client.util.ClientUtil.revisions;
import store.client.util.Directories;
import store.common.client.RepositoryClient;
import store.common.exception.NodeException;
import store.common.hash.Digest;
import static store.common.metadata.MetadataUtil.metadata;
import store.common.metadata.Properties.Common;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentState;
import static store.common.model.ContentState.PRESENT;
import static store.common.model.ContentState.STAGING;
import store.common.model.Revision;
import store.common.model.Revision.RevisionBuilder;
import store.common.model.StagingInfo;
import static store.common.util.IoUtil.copyAndDigest;
import static store.common.util.SinkOutputStream.sink;
import store.common.value.Value;

class Put extends AbstractCommand {

    Put() {
        super(Category.CONTENTS, Type.PATH);
    }

    @Override
    public String description() {
        return "Put a new content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Path path = Directories.resolve(params.get(0));
        if (!Files.exists(path)) {
            throw new RequestFailedException(path + " does not exist");
        }
        ContentUploader uploader = new ContentUploader(display, config, session.getRepository());
        if (!Files.isDirectory(path)) {
            CommandResult result = uploader.put(path, Collections.<String, Value>emptyMap());
            display.print(result);
            return;
        }
        try {
            Files.walkFileTree(path,
                               EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                               Integer.MAX_VALUE,
                               new UploadingVisitor(display, uploader, path));

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    /**
     * Support class for content uploading.
     */
    private static class ContentUploader {

        private final Display display;
        private final ClientConfig config;
        private final RepositoryClient client;

        /**
         * Constructor.
         *
         * @param display Display.
         * @param config Config.
         * @param client Repository client to use.
         */
        public ContentUploader(Display display, ClientConfig config, RepositoryClient client) {
            this.display = display;
            this.config = config;
            this.client = client;
        }

        /**
         * Extracts metadata from the file at supplied path. Adds supplied metadata to this set. Then creates a new
         * content in current repository by uploading this file with extracted metadata along.
         *
         * @param filepath File path.
         * @param metadata Ad-hoc metadata to add.
         * @return The result of this operation.
         */
        public CommandResult put(Path filepath, Map<String, Value> metadata) {
            try {
                Digest digest = digest(filepath);
                ContentInfo contentInfo = client.getContentInfo(digest.getHash());
                checkState(contentInfo);

                Revision revision = new RevisionBuilder()
                        .withContent(digest.getHash())
                        .withLength(digest.getLength())
                        .withParents(revisions(contentInfo.getHead()))
                        .withMetadata(metadata(filepath))
                        .withMetadata(metadata)
                        .computeRevisionAndBuild();

                if (contentInfo.getState() != ContentState.STAGED) {
                    addContent(filepath, digest);
                }
                return client.addRevision(revision);

            } catch (IOException e) {
                throw new RequestFailedException(e);
            }
        }

        private static void checkState(ContentInfo contentInfo) {
            switch (contentInfo.getState()) {
                case STAGING:
                    throw new RequestFailedException(
                            "There is already another staging session in progress for this content");

                case PRESENT:
                    throw new RequestFailedException("This content is already stored");

                default:
                // Others cases are fine.
            }
        }

        private Digest digest(Path filepath) throws IOException {
            try (InputStream inputStream = read("Computing content digest", filepath, size(filepath))) {
                return copyAndDigest(inputStream, sink());
            }
        }

        private void addContent(Path filepath, Digest digest) throws IOException {
            StagingInfo stagingInfo = client.stageContent(digest.getHash());
            try (InputStream inputStream = read("Uploading content", filepath, digest.getLength())) {
                inputStream.skip(stagingInfo.getLength());
                client.writeContent(digest.getHash(), stagingInfo.getSessionId(), inputStream, stagingInfo.getLength());

            } finally {
                client.unstageContent(digest.getHash(), stagingInfo.getSessionId());
            }
        }

        private InputStream read(String task, Path filepath, long length) throws IOException {
            return new LoggingInputStream(display, config, task, newInputStream(filepath), length);
        }
    }

    /**
     * A file tree visitor which uploads a content for each file found.
     */
    private static class UploadingVisitor extends SimpleFileVisitor<Path> {

        private final Display display;
        private final ContentUploader uploader;
        private final Path root;

        /**
         * Constructor.
         *
         * @param display Display.
         * @param uploader Content uploader to use.
         * @param path Root path of the tree to visit.
         */
        public UploadingVisitor(Display display, ContentUploader uploader, Path path) {
            this.display = display;
            this.uploader = uploader;
            root = path.getParent();
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            if (attributes.isRegularFile()) {
                Path relative = root.relativize(file);
                display.println("Processing: " + relative);
                try {
                    Map<String, Value> metadata = ImmutableMap.of(Common.PATH.key(),
                                                                  Value.of(relative.getParent().toString()));

                    CommandResult result = uploader.put(file, metadata);
                    display.print(result);

                } catch (NodeException e) {
                    display.print(e);
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }

    /**
     * An input stream that prints read progression.
     */
    private static class LoggingInputStream extends InputStream {

        private static final int EOF = -1;
        private final Display display;
        private final ClientConfig config;
        private final String task;
        private InputStream inputStream;
        private final long length;
        private long read;
        private int currentProgress;
        private boolean closed;

        /**
         * Constructor.
         *
         * @param display Display.
         * @param config Config.
         * @param task Task description to print.
         * @param inputStream Underlying input-stream.
         * @param length Expected total length to read.
         */
        public LoggingInputStream(Display display, ClientConfig config, String task, InputStream inputStream,
                                  long length) {
            this.display = display;
            this.config = config;
            this.task = task;
            this.inputStream = inputStream;
            this.length = length;
        }

        private void increment(long n) {
            read += n;
            int newProgress = (int) ((read * 100.0d) / length);
            if (newProgress != currentProgress) {
                currentProgress = newProgress;
                if (config.isDisplayProgress()) {
                    display.print(task + " " + currentProgress + "%\r");
                }
            }
        }

        @Override
        public int read() throws IOException {
            int res = inputStream.read();
            if (res != EOF) {
                increment(1);
            }
            return res;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int res = inputStream.read(b, off, len);
            if (res != EOF) {
                increment(res);
            }
            return res;
        }

        @Override
        public long skip(long n) throws IOException {
            increment(n);
            return inputStream.skip(n);
        }

        @Override
        public int available() throws IOException {
            return inputStream.available();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public synchronized void mark(int readlimit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                if (config.isDisplayProgress()) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < task.length() + " 100%".length(); i++) {
                        builder.append(" ");
                    }
                    builder.append('\r');
                    display.print(builder.toString());
                }
                inputStream.close();
            }
        }
    }
}
