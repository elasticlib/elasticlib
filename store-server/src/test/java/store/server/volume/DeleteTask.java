package store.server.volume;

import store.common.Hash;
import store.server.Content;

public class DeleteTask extends Task {

    private final Volume volume;
    private final Hash hash;

    public DeleteTask(Volume volume, Content content) {
        this.volume = volume;
        this.hash = content.getHash();
    }

    @Override
    protected void execute() {
        volume.delete(hash);
    }
}
