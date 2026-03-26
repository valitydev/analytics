package dev.vality.analytics.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class BatchRetryErrorListener implements RetryErrorListener {

    @Value("${kafka.listener.retry.timeout}")
    private long retryTimeoutMs;

    public void retry(String listenerName, int batchSize, Acknowledgment ack, Exception ex) {
        log.warn("{} batch processing failed, size={}, retry in {} ms", listenerName, batchSize, retryTimeoutMs, ex);
        ack.nack(0, Duration.ofMillis(retryTimeoutMs));
    }
}
