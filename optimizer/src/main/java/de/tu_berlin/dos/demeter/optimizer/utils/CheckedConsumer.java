package de.tu_berlin.dos.demeter.optimizer.utils;

@FunctionalInterface
public interface CheckedConsumer<T> {

    void accept(T t) throws Exception;
}
