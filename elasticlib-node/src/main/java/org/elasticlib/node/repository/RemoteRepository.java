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
package org.elasticlib.node.repository;

import static com.google.common.base.Joiner.on;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.String.join;
import java.net.SocketException;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.ws.rs.ProcessingException;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.client.RepositoryClient;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * View of a remote repository.
 */
public class RemoteRepository implements Repository {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteRepository.class);

    private final Client client;
    private final String name;
    private final Guid guid;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean closed = false;

    /**
     * Constructor.
     *
     * @param client HTTP client on the remote repository.
     * @param name Remote repository name.
     * @param guid Remote repository GUID.
     */
    public RemoteRepository(Client client, String name, Guid guid) {
        this.client = client;
        this.name = name;
        this.guid = guid;
    }

    @Override
    public RepositoryDef getDef() {
        return readLocked(() -> {
            log("Returning repository def");
            return getInfo().getDef();
        });
    }

    @Override
    public RepositoryInfo getInfo() {
        return readLocked(() -> {
            log("Returning repository info");
            return repository().getInfo();
        });
    }

    @Override
    public StagingInfo stageContent(Hash hash) {
        return readLocked(() -> {
            log("Staging content {}", hash);
            return repository().stageContent(hash);
        });
    }

    @Override
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream source, long position) {
        return readLocked(() -> {
            log("Writing to staged content {}", hash);
            return repository().writeContent(hash, sessionId, source, position);
        });
    }

    @Override
    public void unstageContent(Hash hash, Guid sessionId) {
        readLocked(() -> {
            log("Ending staging staging session {}", sessionId);
            repository().unstageContent(hash, sessionId);
            return null;
        });
    }

    @Override
    public CommandResult addRevision(Revision revision) {
        return readLocked(() -> {
            log("Adding revision to {}, with head {}", revision.getContent(), revision.getParents());
            return repository().addRevision(revision);
        });
    }

    @Override
    public CommandResult mergeTree(RevisionTree tree) {
        return readLocked(() -> {
            log("Merging revision tree of {}", tree.getContent());
            return repository().mergeTree(tree);
        });
    }

    @Override
    public CommandResult deleteContent(Hash hash, SortedSet<Hash> head) {
        return readLocked(() -> {
            log("Deleting content {}, with head {}", hash, head);
            return repository().deleteContent(hash, head);
        });
    }

    @Override
    public ContentInfo getContentInfo(Hash hash) {
        return readLocked(() -> {
            log("Returning content info of {}", hash);
            return repository().getContentInfo(hash);
        });
    }

    @Override
    public RevisionTree getTree(Hash hash) {
        return readLocked(() -> {
            log("Returning revision tree of {}", hash);
            return repository().getTree(hash);
        });
    }

    @Override
    public List<Revision> getHead(Hash hash) {
        return readLocked(() -> {
            log("Returning head revisions of {}", hash);
            return repository().getHead(hash);
        });
    }

    @Override
    public List<Revision> getRevisions(Hash hash, Collection<Hash> revs) {
        return readLocked(() -> {
            log("Returning revisions of {} [{}]", hash, on(", ").join(revs));
            return repository().getRevisions(hash, revs);
        });
    }

    @Override
    public InputStream getContent(Hash hash, long offset, long length) {
        return readLocked(() -> {
            log("Returning content {}, offset {}, length {}", hash, offset, length);
            InputStream inputStream = repository().getContent(hash, offset, length).getInputStream();
            return new RemoteContent(inputStream);
        });
    }

    @Override
    public Digest getDigest(Hash hash) {
        return readLocked(() -> {
            log("Returning digest of content {}", hash);
            return repository().getDigest(hash);
        });
    }

    @Override
    public Digest getDigest(Hash hash, long offset, long length) {
        return readLocked(() -> {
            log("Returning digest of content {}, offset {}, length {}", hash, offset, length);
            return repository().getDigest(hash, offset, length);
        });
    }

    @Override
    public List<Event> history(boolean chronological, long first, int number) {
        return readLocked(() -> {
            log("Returning history, {}, first {}, number {}", chronological ? "asc" : "desc", first, number);
            return repository().history(chronological, first, number);
        });
    }

    @Override
    public List<IndexEntry> find(String query, int first, int number) {
        return readLocked(() -> {
            log("Returning index entries for '{}', first {}, number {}", query, first, number);
            return repository().find(query, first, number);
        });
    }

    private RepositoryClient repository() {
        return client.repositories().get(guid);
    }

    private <T> T readLocked(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            if (closed) {
                throw new RepositoryClosedException();
            }
            return supplier.get();

        } catch (ProcessingException e) {
            if (e.getCause() instanceof SocketException) {
                throw new UnreachableNodeException(e);
            }
            throw e;

        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            log("Closing");
            closed = true;
            client.close();

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void log(String format, Object... args) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info(join("", "[", name, "] ", format), args);
    }

    /**
     * Wraps a remote content input stream, providing adequate locking.
     */
    private class RemoteContent extends InputStream {

        private final InputStream delegate;

        /**
         * Constructor.
         *
         * @param delegate Wrapped input stream.
         */
        public RemoteContent(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() {
            return readLocked(() -> {
                if (closed) {
                    throw new RepositoryClosedException();
                }
                return delegate.read();
            });
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return readLocked(() -> {
                if (closed) {
                    throw new RepositoryClosedException();
                }
                return delegate.read(b, off, len);
            });
        }

        @Override
        public void close() {
            readLocked(() -> {
                delegate.close();
                return 0;
            });
        }

        private int readLocked(IOReader reader) {
            lock.readLock().lock();
            try {
                try {
                    return reader.read();

                } catch (ProcessingException e) {
                    if (e.getCause() instanceof SocketException) {
                        throw new UnreachableNodeException(e);
                    }
                    throw e;

                } catch (IOException e) {
                    throw new IOFailureException(e);
                }
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Defines a method that reads bytes from a stream.
     */
    @FunctionalInterface
    private interface IOReader {

        /**
         * Applies this method.
         *
         * @return the number of bytes read.
         * @throws IOException If an I/O error occurs.
         */
        int read() throws IOException;
    }
}
