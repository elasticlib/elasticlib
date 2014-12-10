/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.mappable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toCollection;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.value.Value;

/**
 * Utilities to deal with mappables.
 */
public final class MappableUtil {

    private static final String REVISION = "revision";
    private static final String REVISIONS = "revisions";

    private MappableUtil() {
    }

    /**
     * Converts supplied set of hashes to a list of values.
     *
     * @param hashes A set of hashes.
     * @return Corresponding list of values.
     */
    public static List<Value> toList(SortedSet<Hash> hashes) {
        return hashes.stream()
                .map(hash -> Value.of(hash))
                .collect(Collectors.toList());
    }

    /**
     * Extracts a set of hashes from supplied list of values. All values are expected to be hash ones.
     *
     * @param values A list of values.
     * @return Corresponding set of hashes
     */
    public static SortedSet<Hash> fromList(List<Value> values) {
        return values.stream()
                .map(value -> value.asHash())
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Adds supplied revisions to builder, with adequate key.
     *
     * @param builder A map builder.
     * @param revisions Revisions to add.
     * @return Supplied builder instance.
     */
    public static MapBuilder putRevisions(MapBuilder builder, SortedSet<Hash> revisions) {
        if (revisions.size() == 1) {
            return builder.put(REVISION, revisions.first());
        }
        return builder.put(REVISIONS, toList(revisions));
    }

    /**
     * Extracts a set of revisions from supplied map of values.
     *
     * @param values A map of values.
     * @return A set of revision hashes.
     */
    public static SortedSet<Hash> revisions(Map<String, Value> values) {
        if (values.containsKey(REVISION)) {
            SortedSet<Hash> revisions = new TreeSet<>();
            revisions.add(values.get(REVISION).asHash());
            return revisions;

        } else {
            return fromList(values.get(REVISIONS).asList());
        }
    }

    /**
     * Converts supplied map of values to a {@link Mappable} instance.
     *
     * @param <T> Actual class to convert to.
     * @param values A map of values.
     * @param clazz Actual class to convert to.
     * @return A new instance of supplied class.
     */
    public static <T extends Mappable> T fromMap(Map<String, Value> values, Class<T> clazz) {
        try {
            Method method = clazz.getMethod("fromMap", Map.class);
            return clazz.cast(method.invoke(null, values));

        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
