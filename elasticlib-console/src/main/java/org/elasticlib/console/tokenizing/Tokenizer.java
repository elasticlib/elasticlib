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
package org.elasticlib.console.tokenizing;

import java.util.ArrayList;
import java.util.List;
import static org.elasticlib.console.tokenizing.LexemeType.QUOTE;

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
