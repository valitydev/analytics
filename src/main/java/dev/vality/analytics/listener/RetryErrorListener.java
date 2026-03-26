package dev.vality.analytics.listener;

import org.springframework.kafka.support.Acknowledgment;

public interface RetryErrorListener {
    void retry(String listenerName, int batchSize, Acknowledgment ack, Exception ex);
}
