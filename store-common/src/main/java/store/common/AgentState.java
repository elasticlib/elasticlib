package store.common;

/**
 * Defines all possible agent states.
 */
public enum AgentState {

    /**
     * Agent has not been started yet.
     */
    NEW,
    /**
     * Agent is up and doing some work.
     */
    RUNNING,
    /**
     * Agent is up but waiting for more work.
     */
    WAITING,
    /**
     * Agent is down, due to an unexpected error.
     */
    ERROR;

    /**
     * Provides state matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg A state as a string, as obtained by a call to toString().
     * @return Corresponding state.
     */
    public static AgentState fromString(String arg) {
        return AgentState.valueOf(arg.toUpperCase());
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
