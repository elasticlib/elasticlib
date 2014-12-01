package store.client.tokenizing;

import static com.google.common.collect.Iterables.getLast;
import java.util.List;
import static java.util.stream.Collectors.toList;

/**
 * Command line tokenizing utils.
 */
public final class Tokenizing {

    private Tokenizing() {
    }

    /**
     * Extract argument list from supplied command line buffer.
     *
     * @param buffer Command line buffer.
     * @return Corresponding argument list.
     */
    public static List<String> argList(String buffer) {
        return arguments(buffer)
                .stream()
                .map(Argument::getValue)
                .collect(toList());
    }

    /**
     * Provides position of the last argument in supplied command line buffer (or zero if buffer is empty).
     *
     * @param buffer Command line buffer.
     * @return Cursor position of its last argument.
     */
    public static int lastArgumentPosition(String buffer) {
        List<Argument> arguments = arguments(buffer);
        if (arguments.isEmpty()) {
            return 0;
        }
        return getLast(arguments).getPosition();
    }

    private static List<Argument> arguments(String buffer) {
        List<Lexeme> lexemes = new Lexer(buffer).lex();
        return new Tokenizer(lexemes).tokenize();
    }

    /**
     * Checks if supplied buffer contains a valid argument list.
     *
     * @param buffer Command line buffer.
     * @return true if supplied buffer does not have unclosed quote.
     */
    public static boolean isComplete(String buffer) {
        List<Lexeme> lexemes = new Lexer(buffer).lex();
        int i = 0;
        boolean inQuote = false;
        while (i < lexemes.size()) {
            LexemeType current = lexemes.get(i).getType();
            LexemeType next = i + 1 < lexemes.size() ? lexemes.get(i + 1).getType() : null;

            if (current != LexemeType.QUOTE) {
                i++;

            } else if (next != LexemeType.QUOTE) {
                inQuote = !inQuote;
                i++;

            } else if (inQuote) {
                i += 2;
            } else {
                inQuote = true;
                i++;
            }
        }
        return !inQuote;
    }

    /**
     * Escape quotes in supplied argument.
     *
     * @param argument Command line argument.
     * @return Corresponding escaped argument.
     */
    public static String escape(String argument) {
        if (!containsAny(argument, Chars.SPACE, Chars.LF, Chars.CR, Chars.QUOTE)) {
            return argument;
        }
        List<Lexeme> lexemes = new Lexer(argument).lex();
        StringBuilder builder = new StringBuilder();
        builder.append(Chars.QUOTE);
        Lexeme first = lexemes.get(0);
        if (first.getType() != LexemeType.QUOTE) {
            builder.append(first);
        }
        int i = 1;
        while (i < lexemes.size() - 1) {
            Lexeme current = lexemes.get(i);
            if (current.getType() != LexemeType.QUOTE) {
                builder.append(current);
                i++;

            } else {
                builder.append("''");
                i += lexemes.get(i + 1).getType() == LexemeType.QUOTE ? 2 : 1;
            }
        }
        Lexeme last = getLast(lexemes);
        if (last.getType() != LexemeType.QUOTE) {
            builder.append(last);
        }
        builder.append(Chars.QUOTE);
        return builder.toString();
    }

    private static boolean containsAny(String text, char... characters) {
        for (char c : characters) {
            if (text.indexOf(c) >= 0) {
                return true;
            }
        }
        return false;
    }
}
