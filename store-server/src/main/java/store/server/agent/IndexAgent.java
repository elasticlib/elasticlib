package store.server.agent;

import java.io.IOException;
import java.io.PipedInputStream;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.Operation.DELETE;
import static store.common.Operation.PUT;
import store.server.Index;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import store.server.volume.Volume;

class IndexAgent extends Agent {

    private final Index index;

    public IndexAgent(Volume volume, Index index) {
        super(volume);
        this.index = index;
    }

    @Override
    protected AgentThread newAgentThread() {
        return this.new IndexAgentThread();
    }

    private class IndexAgentThread extends AgentThread {

        @Override
        protected boolean process(Event event) {
            try {
                switch (event.getOperation()) {
                    case PUT:
                        put(event.getHash());
                        break;
                    case DELETE:
                        delete(event.getHash());
                        break;
                }
                return true;

            } catch (UnknownHashException | VolumeClosedException e) {
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
