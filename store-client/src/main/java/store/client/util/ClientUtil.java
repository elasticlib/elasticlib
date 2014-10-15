package store.client.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import store.client.exception.RequestFailedException;
import store.common.client.Client;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.Revision;

/**
 * Client utilities.
 */
public final class ClientUtil {

    private static final String HTTP_SCHEME = "http://";

    private ClientUtil() {
    }

    /**
     * Safely parses supplied argument as a Hash.
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
     * Safely parses supplied argument as an URI.
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
     * Lists revisions hashes of supplied head.
     *
     * @param head A list of revisions.
     * @return Their revisions hashes.
     */
    public static Set<Hash> revisions(List<Revision> head) {
        Set<Hash> revs = new HashSet<>();
        for (Revision info : head) {
            revs.add(info.getRevision());
        }
        return revs;
    }

    /**
     * Resolves the GUID of a repository, using supplied client.
     *
     * @param client Node client.
     * @param repository Repository name or encoded GUID.
     * @return GUID of this repository.
     */
    public static Guid resolveRepositoryGuid(Client client, String repository) {
        return client.repositories()
                .getInfo(repository)
                .getDef()
                .getGuid();
    }
}
