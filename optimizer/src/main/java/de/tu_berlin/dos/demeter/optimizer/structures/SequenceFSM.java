package de.tu_berlin.dos.demeter.optimizer.structures;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface SequenceFSM<C, E extends Enum<E> & SequenceFSM<C, E>> {

    Logger LOG = LogManager.getLogger(SequenceFSM.class);

    E runStage(C context) throws Exception;

    default void run(Class<E> definition, C ctx) throws Exception {

        E[] stages = definition.getEnumConstants();
        E finalState = stages[stages.length - 1];
        E curState = stages[0];

        while (curState != finalState) {

            E prev = curState;
            curState = curState.runStage(ctx);
            LOG.info("STATE-CHANGE: " + prev + " -> " + curState);
        }
    }
}
