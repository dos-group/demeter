package de.tu_berlin.dos.demeter.optimizer.clients.kubernetes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public enum Helm { get;

    /******************************************************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************************************************/

    private static class StreamConsumer implements Runnable {

        private final InputStream inputStream;
        private final Consumer<String> consumer;

        public StreamConsumer(InputStream inputStream, Consumer<String> consumer) {

            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {

            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
        }
    }

    public static class CommandBuilder {

        public enum Command {

            INSTALL("install"), UNINSTALL("uninstall");

            private final String command;

            Command(String command) {

                this.command = command;
            }
        }

        private Command command;
        private String name;
        private String chart;
        private String namespace;
        private final Map<String, List<String>> flags;

        private CommandBuilder() {

            this.flags = new LinkedHashMap<>();
        }

        public CommandBuilder setCommand(Command command) {

            this.command = command;
            return this;
        }

        public CommandBuilder setName(String name) {

            this.name = name.strip();
            return this;
        }

        public CommandBuilder setChart(String chart) {

            this.chart = chart.strip();
            return this;
        }

        public CommandBuilder setNamespace(String namespace) {

            this.namespace = namespace.strip();
            return this;
        }

        public CommandBuilder setFlag(String key, String value) {

            List<String> values = new ArrayList<>();
            if (this.flags.containsKey(key.strip())) values = this.flags.get(key.strip());
            values.add(value.strip());
            this.flags.put(key.strip(), values);
            return this;
        }

        public String build() {

            StringBuilder sb = new StringBuilder();

            switch (this.command) {

                case INSTALL -> {

                    if (this.name == null) throw new IllegalStateException("Helm install: name undefined");
                    if (this.chart == null) throw new IllegalStateException("Helm install: chart undefined");
                    sb.append(String.format("%s %s %s %s", BINARY_LOCATION, this.command.command, this.name, this.chart));
                }
                case UNINSTALL -> {

                    if (this.name == null) throw new IllegalStateException("Helm uninstall: name undefined");
                    sb.append(String.format("%s %s %s", BINARY_LOCATION, this.command.command, this.name));
                }
                default -> throw new IllegalStateException("Helm: command undefined");
            }

            if (this.namespace != null) sb.append(String.format(" -n %s", this.namespace));

            for (Map.Entry<String, List<String>> entry : this.flags.entrySet()) {

                for (String value : entry.getValue()) {

                    sb.append(String.format(" %s %s", entry.getKey(), value));
                }
            }
            System.out.println(sb.toString());
            return sb.toString();
        }

        public static CommandBuilder builder() {

            return new CommandBuilder();
        }
    }

    /******************************************************************************************************************
     * CLASS STATE
     ******************************************************************************************************************/

    private static final Logger LOG = LogManager.getLogger(Helm.class);
    private static final String BINARY_LOCATION = "binaries/helm";

    /******************************************************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************************************************/

    Helm() { }

    /******************************************************************************************************************
     * INSTANCE BEHAVIOUR(S)
     ******************************************************************************************************************/

    public void execute(String command) throws Exception {

        ExecutorService executor = Executors.newSingleThreadExecutor();

        Runtime rt = Runtime.getRuntime();
        Process process = rt.exec(command);
        StreamConsumer streamConsumer = new StreamConsumer(process.getInputStream(), LOG::info);
        executor.submit(streamConsumer);

        process.waitFor();
        executor.shutdown();
    }
}
