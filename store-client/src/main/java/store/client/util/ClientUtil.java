package store.client.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import store.client.exception.RequestFailedException;
import store.common.hash.Hash;
import store.common.model.ContentInfo;

/**
 * Client utils.
 */
public final class ClientUtil {

    private static final String HTTP_SCHEME = "http://";

    private ClientUtil() {
    }

    /**
     * Safely parse supplied argument as a Hash.
     *
     * @param arg A command line argument.
     * @return Parsed hash.
     */
    public static Hash parseHash(String arg) {
        if (!Hash.isValid(arg)) {
            throw new RequestFailedException("Invalid hash");
        }
        return new Hash(arg);
    }

    /**
     * Safely parse supplied argument as an URI.
     *
     * @param arg A command line argument.
     * @return Parsed URI.
     */
    public static URI parseUri(String arg) {
        if (!arg.startsWith(HTTP_SCHEME)) {
            arg = HTTP_SCHEME + arg;
        }
        try {
            return new URI(arg);

        } catch (URISyntaxException e) {
            throw new RequestFailedException("Invalid URI", e);
        }
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
