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
package org.elasticlib.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import java.util.Map;
import static java.util.Objects.hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Holds info about a live agent.
 */
public final class AgentInfo implements Mappable {

    private static final String CUR_SEQ = "curSeq";
    private static final String MAX_SEQ = "maxSeq";
    private static final String STATE = "state";
    private final long curSeq;
    private final long maxSeq;
    private final AgentState state;

    /**
     * Constructor.
     *
     * @param curSeq The currentSeq attribute.
     * @param maxSeq The maxSeq attribute.
     * @param state The state attribute.
     */
    public AgentInfo(long curSeq, long maxSeq, AgentState state) {
        this.curSeq = curSeq;
        this.maxSeq = maxSeq;
        this.state = state;
    }

    /**
     * @return The latest processed event seq.
     */
    public long getCurSeq() {
        return curSeq;
    }

    /**
     * @return The maximum event seq value.
     */
    public long getMaxSeq() {
        return maxSeq;
    }

    /**
     * @return The agent state.
     */
    public AgentState getState() {
        return state;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(CUR_SEQ, curSeq)
                .put(MAX_SEQ, maxSeq)
                .put(STATE, state.toString())
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static AgentInfo fromMap(Map<String, Value> map) {
        return new AgentInfo(map.get(CUR_SEQ).asLong(),
                             map.get(MAX_SEQ).asLong(),
                             AgentState.fromString(map.get(STATE).asString()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(CUR_SEQ, curSeq)
                .add(MAX_SEQ, maxSeq)
                .add(STATE, state)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(curSeq, maxSeq, state);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AgentInfo)) {
            return false;
        }
        AgentInfo other = (AgentInfo) obj;
        return new EqualsBuilder()
                .append(curSeq, other.curSeq)
                .append(maxSeq, other.maxSeq)
                .append(state, other.state)
                .build();
    }
}
