/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.console.command;

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
import org.elasticlib.common.client.RepositoryClient;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.metadata.MetadataUtil;
import org.elasticlib.common.metadata.Properties.Common;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import static org.elasticlib.common.model.ContentState.PRESENT;
import static org.elasticlib.common.model.ContentState.STAGING;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.util.BoundedInputStream;
import static org.elasticlib.common.util.IoUtil.copyAndDigest;
import static org.elasticlib.common.util.SinkOutputStream.sink;
import org.elasticlib.common.value.Value;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.display.LoggingInputStream;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.revisions;
import org.elasticlib.console.util.Directories;

class Put extends AbstractCommand {

    Put() {
        super(Category.CONTENTS, Type.PATH);
    }

    @Override
    public String description() {
        return "Put a new content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
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
        private final ConsoleConfig config;
        private final RepositoryClient client;

        /**
         * Constructor.
         *
         * @param display Display.
         * @param config Config.
         * @param client Repository client to use.
         */
        public ContentUploader(Display display, ConsoleConfig config, RepositoryClient client) {
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
            try (InputStream inputStream = read("Computing content digest", filepath)) {
                return copyAndDigest(inputStream, sink());
            }
        }

        private Map<String, Value> metadata(Path filepath) throws IOException {
            try (InputStream inputStream = read("Extracting content metadata", filepath)) {
                return MetadataUtil.metadata(filepath, inputStream);
            }
        }

        private void addContent(Path filepath, Digest digest) throws IOException {
            StagingInfo stagingInfo = client.stageContent(digest.getHash());
            try {
                long offset = offset(filepath, stagingInfo);
                try (InputStream inputStream = read("Uploading content", filepath)) {
                    skip(inputStream, offset);
                    client.writeContent(digest.getHash(), stagingInfo.getSessionId(), inputStream, offset);
                }
            } finally {
                client.unstageContent(digest.getHash(), stagingInfo.getSessionId());
            }
        }

        private long offset(Path filepath, StagingInfo stagingInfo) throws IOException {
            long length = stagingInfo.getLength();
            if (length == 0) {
                return 0;
            }
            try (InputStream inputStream = read("Checking content digest", filepath, length)) {
                Hash expected = stagingInfo.getHash();
                Hash actual = copyAndDigest(new BoundedInputStream(inputStream, length), sink()).getHash();

                return expected.equals(actual) ? length : 0;
            }
        }

        private static void skip(InputStream inputStream, long offset) throws IOException {
            long remaining = offset;
            while (remaining > 0) {
                remaining -= inputStream.skip(remaining);
            }
        }

        private InputStream read(String task, Path filepath) throws IOException {
            return read(task, filepath, size(filepath));
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
}
