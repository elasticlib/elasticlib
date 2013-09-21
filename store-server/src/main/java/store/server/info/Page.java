package store.server.info;

import com.google.common.base.Optional;
import static com.google.common.base.Preconditions.checkState;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import store.common.hash.Hash;
import store.common.info.ContentInfo;
import static store.server.info.PageState.*;

final class Page {

    private static final Page UNLOADED_PAGE = new Page(UNLOADED);
    private static final Page LOCKED_PAGE = new Page(LOCKED);
    private static final Page EMPTY_PAGE = new Page(LOADED);
    private final PageState state;
    private final Map<Hash, ContentInfo> info;

    private Page(PageState state, Map<Hash, ContentInfo> info) {
        this.state = state;
        this.info = info;
    }

    private Page(PageState state) {
        this(state, Collections.<Hash, ContentInfo>emptyMap());
    }

    public static Page of(Collection<ContentInfo> info) {
        Map<Hash, ContentInfo> map = new HashMap<>(info.size());
        for (ContentInfo contentInfo : info) {
            map.put(contentInfo.getHash(), contentInfo);
        }
        return new Page(LOADED, map);
    }

    public static Page unloaded() {
        return UNLOADED_PAGE;
    }

    public static Page locked() {
        return LOCKED_PAGE;
    }

    public static Page empty() {
        return EMPTY_PAGE;
    }

    public PageState state() {
        return state;
    }

    public boolean contains(Hash hash) {
        checkState(state == LOADED);
        return info.containsKey(hash);
    }

    public Optional<ContentInfo> get(Hash hash) {
        checkState(state == LOADED);
        if (info.containsKey(hash)) {
            return Optional.of(info.get(hash));
        }
        return Optional.absent();
    }

    public Collection<ContentInfo> getAll() {
        checkState(state == LOADED);
        return info.values();
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, info);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != Page.class) {
            return false;
        }
        final Page other = (Page) obj;
        if (this.state != other.state) {
            return false;
        }
        if (!info.equals(other.info)) {
            return false;
        }
        return true;
    }
}
