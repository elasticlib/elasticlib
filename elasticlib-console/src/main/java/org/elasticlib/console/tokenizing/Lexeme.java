package org.elasticlib.console.tokenizing;

/**
 * Represents a lexeme.
 */
class Lexeme {

    private final LexemeType type;
    private final String value;

    public Lexeme(LexemeType type, String value) {
        this.type = type;
        this.value = value;
    }

    public Lexeme(LexemeType type, char value) {
        this.type = type;
        this.value = "" + value;
    }

    public LexemeType getType() {
        return type;
    }

    @Override
    public String toString() {
        return value;
    }
}
