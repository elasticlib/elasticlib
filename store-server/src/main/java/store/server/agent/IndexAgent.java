package store.server.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.Operation.DELETE;
import store.server.Index;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;
import store.server.volume.Volume;

public class IndexAgent extends Agent {

    private final Volume volume;
    private final Index index;

    public IndexAgent(Volume volume, Index index) {
        this.volume = volume;
        this.index = index;
    }

    @Override
    List<Event> history(boolean chronological, long first, int number) {
        return volume.history(chronological, first, number);
    }

    @Override
    void get(Hash hash, OutputStream outputStream) {
        volume.get(hash, outputStream);
    }

    @Override
    AgentThread newAgentThread() {
        return this.new IndexAgentThread();
    }

    private class IndexAgentThread extends AgentThread {

        @Override
        protected boolean process(Event event) {
            try {
                switch (event.getOperation()) {
                    case CREATE:
                    case RESTORE:
                    case UPDATE:
                        put(event.getHash());
                        break;
                    case DELETE:
                        delete(event.getHash());
                        break;
                }
                return true;

            } catch (UnknownContentException | RepositoryNotStartedException e) {
                return false;
            }
        }

        private void put(Hash hash) {
            if (index.contains(hash)) {
                return;
            }
            ContentInfo info = volume.info(hash);
            try (PipedInputStream in = new PipedInputStream()) {
                PipeWriterThread pipeWriter = new PipeWriterThread(in, hash);
                try {
                    pipeWriter.start();
                    index.put(info, in);

                } catch (RuntimeException e) { // Moche
                    pipeWriter.throwCauseIfAny();
                    throw e;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void delete(Hash hash) {
            index.delete(hash); // Idempotent
        }
    }
}
