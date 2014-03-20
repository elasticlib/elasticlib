package store.server.agent;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.Operation.DELETE;
import store.server.Index;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;
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
    AgentThread newAgentThread() {
        return this.new IndexAgentThread();
    }

    private class IndexAgentThread extends AgentThread {

        @Override
        protected boolean process(Event event) {
            try {
                switch (event.getOperation()) {
                    case CREATE:
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
            ContentInfo info = volume.getInfoHead(hash).get(0);
            try (InputStream inputStream = volume.get(hash)) {
                index.put(info, inputStream);

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        private void delete(Hash hash) {
            index.delete(hash); // Idempotent
        }
    }
}
