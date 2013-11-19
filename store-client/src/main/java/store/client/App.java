package store.client;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
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
        try (Session session = new Session()) {
            String line;
            while ((line = session.getConsoleReader().readLine()) != null) {
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                    break;
                }
                List<String> argList = Lists.newArrayList(Splitter.on(" ").trimResults().split(line));
                Optional<Command> OptCommand = Command.of(argList);
                if (!OptCommand.isPresent()) {
                    session.out().println("Unsupported command !"); // TODO print help
                    session.out().flush();
                } else {
                    try {
                        Command command = OptCommand.get();
                        command.execute(session, command.params(argList));

                    } catch (RequestFailedException e) {
                        session.out().println(e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static enum Command {

        CREATE_VOLUME {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().createVolume(Paths.get(params.get(0)));
            }
        },
        DROP_VOLUME {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().dropVolume(params.get(0));
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
            public void execute(Session session, List<String> params) {
                session.getRestClient().createIndex(Paths.get(params.get(0)), params.get(1));
            }
        },
        DROP_INDEX {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().dropIndex(params.get(0));
            }
        },
        CREATE_REPLICATION {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().createReplication(params.get(0), params.get(1));
            }
        },
        DROP_REPLICATION {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().dropReplication(params.get(0), params.get(1));
            }
        },
        START {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().start();
            }
        },
        STOP {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().stop();
            }
        },
        PUT {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().put(Paths.get(params.get(0)));
            }
        },
        DELETE {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().delete(new Hash(params.get(0)));
            }
        },
        GET {
            @Override
            public void execute(Session session, List<String> params) {
                session.getRestClient().get(new Hash(params.get(0)));
            }
        },
        INFO {
            @Override
            public void execute(Session session, List<String> params) {
                ContentInfo info = session.getRestClient().info(new Hash(params.get(0)));
                session.out().println(asString(info));
            }
        },
        FIND {
            @Override
            public void execute(Session session, List<String> params) {
                String query = Joiner.on(" ").join(params);
                for (ContentInfo info : session.getRestClient().find(query)) {
                    session.out().println(asString(info));
                }
            }
        },
        HISTORY {
            @Override
            public void execute(Session session, List<String> params) {
                long cursor = Long.MAX_VALUE;
                List<Event> events;
                do {
                    events = session.getRestClient().history(cursor, 20);
                    for (Event event : events) {
                        cursor = event.getSeq();
                        session.out().println(asString(event));
                    }
                } while (!events.isEmpty() && cursor > 1);
            }
        },
        DIGEST {
            @Override
            public void execute(Session session, List<String> params) {
                Digest digest = digest(Paths.get(params.get(0)));
                session.out().println(asString(digest));
            }
        },
        SET_VOLUME {
            @Override
            public void execute(Session session, List<String> params) {
                session.setVolume(params.get(0));
            }
        },
        UNSET_VOLUME {
            @Override
            public void execute(Session session, List<String> params) {
                session.unsetVolume();
            }
        };

        public abstract void execute(Session session, List<String> params);

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
