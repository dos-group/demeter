package de.tu_berlin.dos.demeter.optimizer.utils;

@FunctionalInterface
public interface CheckedRunnable {

    void run() throws Exception;
}
