package store.client.display;

import com.google.common.collect.ImmutableMap;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonGenerator;
import jline.console.ConsoleReader;
import store.client.config.ClientConfig;
import static store.client.display.MappableFormatting.format;
import store.common.Mappable;
import store.common.json.JsonWriting;
import store.common.value.Value;
import store.common.yaml.YamlWriting;

/**
 * Console display interface.
 */
public class Display {

    private static final String PROMPT = "> ";
    private final ConsoleReader consoleReader;
    private final ClientConfig config;
    private final PrintWriter out;

    /**
     * Constructor.
     *
     * @param consoleReader Console.
     * @param config Config.
     */
    public Display(ConsoleReader consoleReader, ClientConfig config) {
        this.consoleReader = consoleReader;
        this.config = config;
        out = new PrintWriter(consoleReader.getOutput());
        consoleReader.setPrompt(PROMPT);
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
     * Print supplied exception.
     *
     * @param e Exception to print.
     */
    public void print(Exception e) {
        out.println(e.getMessage() + System.lineSeparator());
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

    private String write(Mappable mappable) {
        switch (config.getDisplayFormat()) {
            case YAML:
                Value value = config.isDisplayPretty() ? format(mappable) : Value.of(mappable.toMap());
                return YamlWriting.writeValue(value);
            case JSON:
                return writeJson(JsonWriting.write(mappable));
            default:
                throw new AssertionError();
        }
    }

    private String writeJson(JsonObject json) {
        StringWriter output = new StringWriter();
        Json.createWriterFactory(ImmutableMap.of(JsonGenerator.PRETTY_PRINTING, true))
                .createWriter(output)
                .writeObject(json);
        return output
                .append(System.lineSeparator())
                .toString()
                .substring(1);
    }

    /**
     * Set prompt to display.
     *
     * @param prompt Prompt to display (excluding actual prompt-separator).
     */
    public void setPrompt(String prompt) {
        consoleReader.setPrompt(prompt + PROMPT);
    }

    /**
     * Clear displayed prompt (excluding separator).
     */
    public void resetPrompt() {
        consoleReader.setPrompt(PROMPT);
    }
}
