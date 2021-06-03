package io.appform.dropwizard.actors.base.utils;

import java.util.concurrent.atomic.AtomicLong;

public class ConsumerMeta {

    private AtomicLong totalMessagesProcessed = new AtomicLong(0);

    private AtomicLong activeMessagesCount = new AtomicLong(0);

    public long getActiveMessagesCount() {
        return activeMessagesCount.get();
    }

    public void incrementActiveMessagesCount() {
        this.activeMessagesCount.incrementAndGet();
    }

    public void decrementActiveMessagesCount() {
        this.activeMessagesCount.decrementAndGet();
    }
}
