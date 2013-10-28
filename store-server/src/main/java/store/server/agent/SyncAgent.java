package store.server.agent;

import java.io.IOException;
import java.io.PipedInputStream;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.common.Uid;
import store.server.exception.ConcurrentOperationException;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import store.server.exception.WriteException;
import store.server.volume.Volume;

public class SyncAgent extends Agent {

    private final AgentManager agentManager;
    private final Uid destinationId;
    private final Volume destination;

    public SyncAgent(AgentManager agentManager, Uid destinationId, Volume source, Volume destination) {
        super(source);
        this.agentManager = agentManager;
        this.destinationId = destinationId;
        this.destination = destination;
    }

    @Override
    protected AgentThread newAgentThread() {
        return this.new VolumeAgentThread();
    }

    private class VolumeAgentThread extends AgentThread {

        private int attempt = 1;

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
                agentManager.signal(destinationId);
                return true;

            } catch (ConcurrentOperationException e) {
                try {
                    Thread.sleep(attempt > 10 ? 10000 : 1000 * attempt);
                    attempt++;
                    return process(event);

                } catch (InterruptedException interrupt) {
                    throw new RuntimeException(interrupt);
                }
            } catch (UnknownHashException | VolumeClosedException e) {
                return false;

            } finally {
                attempt = 1;
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
