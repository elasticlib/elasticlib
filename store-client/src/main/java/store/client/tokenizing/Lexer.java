package store.client.tokenizing;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a command line buffer into a list of lexemes.
 */
class Lexer {

    private final List<Lexeme> lexemes = new ArrayList<>();
    private final StringBuilder builder = new StringBuilder();
    private final String buffer;
    private int index;

    public Lexer(String buffer) {
        this.buffer = buffer;
    }

    public List<Lexeme> lex() {
        while (index < buffer.length()) {
            char current = buffer.charAt(index);
            index++;
            switch (current) {
                case Chars.EXCLAMATION_MARK:
                    add(new Lexeme(LexemeType.EXCLAMATION_MARK, current));
                    break;

                case Chars.QUOTE:
                    add(new Lexeme(LexemeType.QUOTE, current));
                    break;

                case Chars.SPACE:
                case Chars.LF:
                case Chars.CR:
                    add(new Lexeme(LexemeType.BLANK, current));
                    break;

                default:
                    builder.append(current);
            }
        }
        flush();
        return lexemes;
    }

    private void add(Lexeme lexeme) {
        flush();
        lexemes.add(lexeme);
    }

    private void flush() {
        if (builder.length() > 0) {
            lexemes.add(new Lexeme(LexemeType.TEXT, builder.toString()));
            builder.delete(0, builder.length());
        }
    }
}
