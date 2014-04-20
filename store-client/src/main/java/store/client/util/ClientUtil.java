package store.client.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import store.client.exception.RequestFailedException;
import store.common.ContentInfo;
import store.common.hash.Hash;

/**
 * Client utils.
 */
public final class ClientUtil {

    private ClientUtil() {
    }

    public static Hash parseHash(String arg) {
        if (!arg.matches("[a-zA-Z0-9]{40}")) {
            throw new RequestFailedException("Invalid hash");
        }
        return new Hash(arg);
    }

    /**
     * Lists revisions of supplied head info.
     *
     * @param head A list of content infos.
     * @return Their revisions.
     */
    public static Set<Hash> revisions(List<ContentInfo> head) {
        Set<Hash> revs = new HashSet<>();
        for (ContentInfo info : head) {
            revs.add(info.getRevision());
        }
        return revs;
    }

    /**
     * Checks if supplied head is deleted.
     *
     * @param head A list of content infos.
     * @return true if head does not contains any not-deleted info.
     */
    public static boolean isDeleted(List<ContentInfo> head) {
        for (ContentInfo info : head) {
            if (!info.isDeleted()) {
                return false;
            }
        }
        return true;
    }
}
