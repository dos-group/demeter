package de.tu_berlin.dos.demeter.optimizer.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class EventTimer {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Listener {

        private static final Logger LOG = LogManager.getLogger(Listener.class);

        private final EventTimer eventTimer;
        private final String id;
        private final ExecutorService executor;
        private final Predicate<Integer> condition;
        private final CheckedConsumer<Listener> callback;

        public Listener(
                EventTimer eventTimer, String id, ExecutorService executor,
                Predicate<Integer> condition, CheckedConsumer<Listener> callback) {

            this.eventTimer = eventTimer;
            this.id = id;
            this.executor = executor;
            this.condition = condition;
            this.callback = callback;
        }

        public boolean test(int index) {

            return this.condition.test(index);
        }

        public void update() {

            executor.execute(() -> {
                try {

                    this.callback.accept(this);
                }
                catch (Throwable t) {

                    LOG.error(t);
                }
            });
        }

        public void remove() {

            this.eventTimer.removeListener(id);
        }
    }

    public static final Logger LOG = LogManager.getLogger(EventTimer.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final ExecutorService executor;
    private final AtomicInteger counter = new AtomicInteger(1);
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final Map<String, Listener> listeners = new TreeMap<>();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public EventTimer(ExecutorService executor) {

        this.executor = executor;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void start(int duration) throws Exception {

        start(duration, false);
    }

    public void start(int duration, boolean whileHasListeners) throws Exception {

        this.resetCounter(0);
        this.isStopped.set(false);
        for (int i = 0; i < duration; i++) {

            long currTsSec = System.currentTimeMillis() / 1000L;
            while ((System.currentTimeMillis() / 1000L) <= currTsSec) {

                new CountDownLatch(1).await(10, TimeUnit.MILLISECONDS);
            }
            // increment counter and check for listeners
            this.incrementCounter();
            // check whether the timer has been stopped
            if (whileHasListeners && this.listeners.isEmpty()) break;
        }
    }

    public String register(Predicate<Integer> condition, CheckedConsumer<Listener> callback) {

        String id = RandomStringUtils.random(10, true, true);
        this.listeners.put(id, new Listener(this, id, executor, condition, callback));
        return id;
    }

    public int getCounter() {

        return this.counter.get();
    }

    public void resetCounter(int counter) {

        this.counter.set(counter);
    }

    public EventTimer clearListeners() {

        this.listeners.clear();
        return this;
    }

    public void incrementCounter() {

        int newVal = this.counter.incrementAndGet();
        this.listeners.forEach((id, listener) -> {

            if (listener.test(newVal)) listener.update();
        });
    }

    public void removeListener(String id) {

        this.listeners.remove(id);
    }

    public void stopTimer() {

        this.isStopped.set(true);
    }
}
