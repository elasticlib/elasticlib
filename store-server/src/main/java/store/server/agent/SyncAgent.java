package store.server.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.server.Repository;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.UnknownHashException;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.WriteException;

class SyncAgent extends Agent {

    private final AgentManager agentManager;
    private final Repository source;
    private final Repository destination;

    public SyncAgent(AgentManager agentManager, Repository source, Repository destination) {
        this.agentManager = agentManager;
        this.source = source;
        this.destination = destination;
    }

    @Override
    List<Event> history(boolean chronological, long first, int number) {
        return source.history(chronological, first, number);
    }

    @Override
    void get(Hash hash, OutputStream outputStream) {
        source.get(hash, outputStream);
    }

    @Override
    AgentThread newAgentThread() {
        return this.new VolumeAgentThread();
    }

    private class VolumeAgentThread extends AgentThread {

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
                agentManager.signal(destination.getName());
                return true;

            } catch (UnknownHashException | RepositoryNotStartedException e) {
                return false;
            }
        }

        private void put(Hash hash) {
            ContentInfo info = source.info(hash);
            try (PipedInputStream in = new PipedInputStream()) {
                PipeWriterThread pipeWriter = new PipeWriterThread(in, hash);
                try {
                    pipeWriter.start();
                    destination.put(info, in);

                } catch (IntegrityCheckingFailedException | WriteException e) {
                    pipeWriter.throwCauseIfAny();
                    throw e;
                } catch (ContentAlreadyStoredException e) {
                    // Ok
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void delete(Hash hash) {
            try {
                destination.delete(hash);

            } catch (UnknownHashException e) {
                // Ok
            }
        }
    }
}
