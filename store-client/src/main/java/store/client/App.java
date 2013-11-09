package store.client;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import static store.client.ByteLengthFormatter.format;
import static store.client.DigestUtil.digest;
import store.common.ContentInfo;
import store.common.Digest;
import store.common.Event;
import store.common.Hash;
import store.common.Properties;
import store.common.Property;

/**
 * Client starting.
 */
public final class App {

    private App() {
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Syntax : store command params...");
            return;
        }
        List<String> argList = Arrays.asList(args);
        Optional<Command> OptCommand = Command.of(argList);
        if (!OptCommand.isPresent()) {
            System.out.println("Unsupported command !"); // TODO print help
            return;
        }
        try {
            Command command = OptCommand.get();
            command.execute(command.params(argList));

        } catch (RequestFailedException e) {
            System.out.println(e.getMessage());
        }
    }

    private static enum Command {

        CREATE_VOLUME {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.createVolume(Paths.get(params.get(0)));
                }
            }
        },
        DROP_VOLUME {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.dropVolume(params.get(0));
                }
            }
        },
        CREATE_INDEX {
            @Override
            public List<String> params(List<String> argList) {
                List<String> params = new ArrayList<>(super.params(argList));
                if (params.get(1).equalsIgnoreCase("on")) {
                    params.remove(1);
                }
                return params;
            }

            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.createIndex(Paths.get(params.get(0)), params.get(1));
                }
            }
        },
        DROP_INDEX {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.dropIndex(params.get(0));
                }
            }
        },
        SYNC {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.sync(params.get(0), params.get(1));
                }
            }
        },
        UNSYNC {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.unsync(params.get(0), params.get(1));
                }
            }
        },
        START {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.start(params.get(0));
                }
            }
        },
        STOP {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.stop(params.get(0));
                }
            }
        },
        PUT {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.put(Paths.get(params.get(0)));
                }
            }
        },
        DELETE {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.delete(new Hash(params.get(0)));
                }
            }
        },
        GET {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    client.get(new Hash(params.get(0)));
                }
            }
        },
        INFO {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    ContentInfo info = client.info(new Hash(params.get(0)));
                    System.out.println(asString(info));
                }
            }
        },
        FIND {
            @Override
            public void execute(List<String> params) {
                String query = Joiner.on(" ").join(params);
                try (RestClient client = new RestClient()) {
                    for (ContentInfo info : client.find(query)) {
                        System.out.println(asString(info));
                    }
                }
            }
        },
        HISTORY {
            @Override
            public void execute(List<String> params) {
                try (RestClient client = new RestClient()) {
                    long cursor = Long.MAX_VALUE;
                    List<Event> events;
                    do {
                        events = client.history(false, cursor, 20);
                        for (Event event : events) {
                            cursor = event.getSeq();
                            System.out.println(asString(event));
                        }
                    } while (!events.isEmpty() && cursor > 1);
                }
            }
        },
        DIGEST {
            @Override
            public void execute(List<String> params) {
                Digest digest = digest(Paths.get(params.get(0)));
                System.out.println(asString(digest));
            }
        };

        public abstract void execute(List<String> params);

        public static Optional<Command> of(List<String> argList) {
            for (Command command : Command.values()) {
                if (command.matches(argList)) {
                    return Optional.of(command);
                }
            }
            return Optional.absent();
        }

        private boolean matches(List<String> argList) {
            Iterator<String> it = argList.iterator();
            for (String part : Splitter.on("_").split(name())) {
                if (!it.hasNext() || !it.next().equalsIgnoreCase(part)) {
                    return false;
                }
            }
            return true;
        }

        public List<String> params(List<String> argList) {
            if (name().contains("_")) {
                return argList.subList(2, argList.size());
            }
            return argList.subList(1, argList.size());
        }
    }

    private static String asString(ContentInfo info) {
        StringBuilder builder = new StringBuilder();
        builder.append("Hash")
                .append(comma())
                .append(info.getHash())
                .append(System.lineSeparator())
                .append("Length")
                .append(comma())
                .append(format(info.getLength()));

        Map<String, Object> metadata = info.getMetadata();
        for (Property property : Properties.list()) {
            if (metadata.containsKey(property.key())) {
                builder.append(System.lineSeparator())
                        .append(property.label())
                        .append(comma())
                        .append(metadata.get(property.key()));
            }
        }
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    private static String asString(Digest digest) {
        return new StringBuilder()
                .append("Hash")
                .append(comma())
                .append(digest.getHash())
                .append(System.lineSeparator())
                .append("Size")
                .append(comma())
                .append(format(digest.getLength()))
                .toString();
    }

    private static String asString(Event event) {
        return new StringBuilder()
                .append(event.getOperation().toString())
                .append(System.lineSeparator())
                .append(indent())
                .append("Hash")
                .append(comma())
                .append(event.getHash())
                .append(System.lineSeparator())
                .append(indent())
                .append("Date")
                .append(comma())
                .append(event.getTimestamp())
                .append(System.lineSeparator())
                .toString();
    }

    private static String indent() {
        return "    ";
    }

    private static String comma() {
        return " : ";
    }
}
