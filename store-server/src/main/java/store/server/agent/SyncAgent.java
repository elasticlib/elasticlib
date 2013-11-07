package store.server.agent;

import java.io.IOException;
import java.io.PipedInputStream;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import store.server.exception.WriteException;
import store.server.volume.Volume;

class SyncAgent extends Agent {

    private final AgentManager agentManager;
    private final Volume destination;

    public SyncAgent(AgentManager agentManager, Volume source, Volume destination) {
        super(source);
        this.agentManager = agentManager;
        this.destination = destination;
    }

    @Override
    protected AgentThread newAgentThread() {
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

            } catch (UnknownHashException | VolumeClosedException e) {
                return false;
            }
        }

        private void put(Hash hash) {
            ContentInfo info = volume.info(hash);
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
