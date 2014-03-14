package store.server.agent;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.server.Repository;
import store.server.RevSpec;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;
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
    AgentThread newAgentThread() {
        return this.new VolumeAgentThread();
    }

    private class VolumeAgentThread extends AgentThread {

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
                agentManager.signal(destination.getName());
                return true;

            } catch (UnknownContentException | RepositoryNotStartedException e) {
                return false;
            }
        }

        private void put(Hash hash) {
            ContentInfo info = source.info(hash);
            try (InputStream inputStream = source.get(hash)) {
                destination.put(info, inputStream, RevSpec.any());

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        private void delete(Hash hash) {
            try {
                destination.delete(hash, RevSpec.any());

            } catch (UnknownContentException e) {
                // Ok
            }
        }
    }
}
