package dev.vality.analytics.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class BatchRetryErrorListener implements RetryErrorListener {

    private final long retryTimeoutMs;

    public BatchRetryErrorListener(
            @Value("${kafka.listener.retry.timeout}") long retryTimeoutMs) {
        this.retryTimeoutMs = retryTimeoutMs;
    }

    public void retry(String listenerName, int batchSize, Acknowledgment ack, Exception ex) {
        log.warn("{} batch processing failed, size={}, retry in {} ms", listenerName, batchSize, retryTimeoutMs, ex);
        ack.nack(0, Duration.ofMillis(retryTimeoutMs));
    }
}
