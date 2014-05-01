package store.client.tokenizing;

import java.util.ArrayList;
import java.util.List;
import static store.client.tokenizing.LexemeType.QUOTE;

/**
 * Assembles list of lexemes into a list of tokens.
 */
class Tokenizer {

    private final List<Lexeme> lexemes;
    private final List<Argument> argList = new ArrayList<>();
    private final StringBuilder builder = new StringBuilder();
    private int index;
    private int position;

    public Tokenizer(List<Lexeme> lexemes) {
        this.lexemes = lexemes;
    }

    public List<Argument> tokenize() {
        while (index < lexemes.size()) {
            Lexeme lexeme = lexemes.get(index);
            index++;
            switch (lexeme.getType()) {
                case QUOTE:
                    addQuote();
                    break;

                case EXCLAMATION_MARK:
                    addExclamationMark();
                    break;

                case BLANK:
                    flush();
                    position++;
                    break;

                default:
                    builder.append(lexeme);
                    break;
            }
        }
        flush();
        return argList;
    }

    private void addQuote() {
        flush();
        int i = index;
        int length = 1;
        while (i < lexemes.size()) {
            Lexeme lexeme = lexemes.get(i);
            if (lexeme.getType() != LexemeType.QUOTE) {
                builder.append(lexeme);
                i++;
                length += lexeme.toString().length();

            } else {
                if (i + 1 < lexemes.size() && lexemes.get(i + 1).getType() == LexemeType.QUOTE) {
                    builder.append(lexeme);
                    i += 2;
                    length += 2;

                } else {
                    length++;
                    break;
                }
            }
        }
        add();
        index = i + 1;
        position += length;
    }

    private void addExclamationMark() {
        builder.append(Chars.EXCLAMATION_MARK);
        if (argList.isEmpty() && builder.length() == 1) {
            add();
            position++;
        }
    }

    private void flush() {
        int length = builder.length();
        if (length > 0) {
            add();
            position += length;
        }
    }

    private void add() {
        argList.add(new Argument(builder.toString(), position));
        builder.delete(0, builder.length());
    }
}
