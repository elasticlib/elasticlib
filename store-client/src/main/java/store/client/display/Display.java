package store.client.display;

import static com.google.common.base.Joiner.on;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.ProcessingException;
import jline.console.ConsoleReader;
import store.client.config.ClientConfig;
import static store.client.display.MappableFormatting.format;
import store.common.client.RequestFailedException;
import store.common.json.JsonWriting;
import store.common.mappable.Mappable;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.value.Value;
import store.common.yaml.YamlWriter;

/**
 * Console display interface.
 */
public class Display {

    private static final String PROMPT = "$ ";
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
     * @param tree A content info tree.
     */
    public void printTree(ContentInfoTree tree) {
        TreePrinter printer = new TreePrinter(2, new RevisionFormatter(config.isDisplayPretty()));
        for (ContentInfo info : tree.list()) {
            printer.add(info);
        }
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
     * @param prompt Prompt to display (excluding actual prompt-separator).
     */
    public void setPrompt(String prompt) {
        if (config.isDisplayColor()) {
            consoleReader.setPrompt(on("").join(Color.BOLD_GREEN, prompt, Color.BOLD_BLUE, PROMPT, Color.RESET));
        } else {
            consoleReader.setPrompt(prompt + PROMPT);
        }
    }

    /**
     * Clear displayed prompt (excluding separator).
     */
    public void resetPrompt() {
        setPrompt("");
    }
}
