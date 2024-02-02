package de.tu_berlin.dos.demeter.optimizer;

public class Run {

    public static void main(String[] args) throws Exception {

        //de.tu_berlin.dos.demeter.optimizer.execution.baseline.Graph.start("baseline.properties");
        de.tu_berlin.dos.demeter.optimizer.execution.demeter.Graph.start("demeter.properties");
        //de.tu_berlin.dos.demeter.optimizer.execution.reactive.Graph.start("reactive.properties");
        //de.tu_berlin.dos.demeter.optimizer.execution.operator.Graph.start("operator.properties");
    }
}
