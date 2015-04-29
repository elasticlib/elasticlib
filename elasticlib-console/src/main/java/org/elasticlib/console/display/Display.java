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
package org.elasticlib.console.display;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.ProcessingException;
import jline.console.ConsoleReader;
import org.elasticlib.common.json.JsonWriting;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.yaml.YamlWriter;
import org.elasticlib.console.config.ConsoleConfig;
import static org.elasticlib.console.display.MappableFormatting.format;
import org.elasticlib.console.exception.RequestFailedException;

/**
 * Console display interface.
 */
public class Display {

    private static final String SEPARATOR = ":";
    private static final String PROMPT = "$ ";

    private final ConsoleReader consoleReader;
    private final ConsoleConfig config;
    private final PrintWriter out;

    /**
     * Constructor.
     *
     * @param consoleReader Console.
     * @param config Config.
     */
    public Display(ConsoleReader consoleReader, ConsoleConfig config) {
        this.consoleReader = consoleReader;
        this.config = config;
        out = new PrintWriter(consoleReader.getOutput());
        consoleReader.setPrompt(PROMPT);
    }

    /**
     * Provides the display underlying console width, in characters.
     *
     * @return The display width.
     */
    public int getWidth() {
        // All terminal implementations are expected to always return a relevant value.
        return consoleReader.getTerminal().getWidth();
    }

    /**
     * Print supplied text.
     *
     * @param text Text to print.
     */
    public void print(String text) {
        out.print(text);
        out.flush();
    }

    /**
     * Print supplied text and append a line-return.
     *
     * @param text Text to print.
     */
    public void println(String text) {
        out.println(text);
        out.flush();
    }

    /**
     * Print "ok" and append a line-return.
     */
    public void printOk() {
        println("ok");
    }

    /**
     * Print supplied exception.
     *
     * @param e Exception to print.
     */
    public void print(RequestFailedException e) {
        out.println(e.getMessage() + System.lineSeparator());
        out.flush();
    }

    /**
     * Print supplied exception.
     *
     * @param e Exception to print.
     */
    public void print(ProcessingException e) {
        String message = Splitter.on(':').limit(2).trimResults().splitToList(e.getMessage()).get(1);
        out.println(message + System.lineSeparator());
        out.flush();
    }

    /**
     * Print supplied mappable and append a line-return.
     *
     * @param mappable Mappable to print.
     */
    public void print(Mappable mappable) {
        println(write(mappable));
    }

    /**
     * Print a text-based representation of supplied tree.
     *
     * @param tree A revision tree.
     */
    public void printTree(RevisionTree tree) {
        TreePrinter printer = new TreePrinter(2, new RevisionFormatter(config.isDisplayPretty()));
        tree.list().forEach(printer::add);
        println(printer.print());
    }

    private String write(Mappable mappable) {
        switch (config.getDisplayFormat()) {
            case YAML:
                Value value = config.isDisplayPretty() ? format(mappable) : Value.of(mappable.toMap());
                return YamlWriter.writeToString(value);
            case JSON:
                return writeJson(JsonWriting.write(mappable));
            default:
                throw new AssertionError();
        }
    }

    private String writeJson(JsonObject json) {
        Map<String, ?> writerConfig = ImmutableMap.of(JsonGenerator.PRETTY_PRINTING, true);
        try (StringWriter writer = new StringWriter();
                JsonWriter jsonWriter = Json.createWriterFactory(writerConfig).createWriter(writer)) {

            jsonWriter.writeObject(json);
            return writer
                    .append(System.lineSeparator())
                    .toString()
                    .substring(1);

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Set prompt to display.
     *
     * @param connectionString Current connection string.
     * @param workingDirectory Current working directory.
     */
    public void setPrompt(String connectionString, Path workingDirectory) {
        consoleReader.setPrompt(prompt(connectionString, workingDirectory));
    }

    private String prompt(String connectionString, Path workingDirectory) {
        StringBuilder builder = new StringBuilder();
        if (!connectionString.isEmpty()) {
            builder.append(part(Color.BOLD_GREEN, connectionString))
                    .append(part(Color.RESET, SEPARATOR));
        }
        return builder.append(part(Color.BOLD_BLUE, workingDirectory.toString()))
                .append(part(Color.RESET, PROMPT))
                .toString();
    }

    private String part(Color color, String value) {
        if (config.isDisplayColor()) {
            return color + value;
        }
        return value;
    }
}
