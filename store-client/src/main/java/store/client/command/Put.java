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
import store.client.http.Session;
import static store.client.util.ClientUtil.isDeleted;
import static store.client.util.ClientUtil.revisions;
import store.client.util.Directories;
import store.client.util.LoggingInputStream;
import store.common.client.RepositoryClient;
import store.common.client.RequestFailedException;
import store.common.hash.Digest;
import static store.common.metadata.MetadataUtil.metadata;
import store.common.metadata.Properties.Common;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.Operation;
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
                List<ContentInfo> head = client.getInfoHeadIfAny(digest.getHash());
                if (!isDeleted(head)) {
                    throw new RequestFailedException("This content is already stored");
                }
                ContentInfo info = new ContentInfo.ContentInfoBuilder()
                        .withContent(digest.getHash())
                        .withLength(digest.getLength())
                        .withParents(revisions(head))
                        .withMetadata(metadata(filepath))
                        .withMetadata(metadata)
                        .computeRevisionAndBuild();

                CommandResult firstStepResult = client.addInfo(info);
                if (firstStepResult.isNoOp() || firstStepResult.getOperation() != Operation.CREATE) {
                    return firstStepResult;
                }
                return putContent(firstStepResult.getTransactionId(), info, filepath);

            } catch (IOException e) {
                throw new RequestFailedException(e);
            }
        }

        private Digest digest(Path filepath) throws IOException {
            try (InputStream inputStream = read("Computing content digest", filepath, size(filepath))) {
                return copyAndDigest(inputStream, sink());
            }
        }

        private CommandResult putContent(long transactionId, ContentInfo info, Path filepath) throws IOException {
            try (InputStream inputStream = read("Uploading content", filepath, info.getLength())) {
                return client.addContent(transactionId, info.getContent(), inputStream);
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

                } catch (RequestFailedException e) {
                    display.print(e);
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }
}
