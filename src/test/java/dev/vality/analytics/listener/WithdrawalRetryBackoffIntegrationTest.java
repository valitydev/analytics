package dev.vality.analytics.listener;

import dev.vality.analytics.config.KafkaConfig;
import dev.vality.analytics.config.SpringBootITest;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventHandler;
import dev.vality.analytics.utils.WithdrawalEventTestUtils;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

@SpringBootITest
public class WithdrawalRetryBackoffIntegrationTest {

    private static final int SUCCESSFUL_ATTEMPT_NUMBER = 3;
    private static final long BACKOFF_TOLERANCE_MS = 200L;

    @Value("${kafka.topic.withdrawal.initial}")
    private String withdrawalTopic;
    @Value("${kafka.error-handler.retry.min.interval}")
    private long minRetryInterval;

    @MockitoBean
    private WithdrawalEventHandler withdrawalEventHandler;
    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldRetryWithdrawalListenerWithExponentialBackoff() {
        AtomicInteger attempts = new AtomicInteger();
        List<Long> attemptTimestamps = new CopyOnWriteArrayList<>();

        Answer<Void> answer = invocation -> {
            attemptTimestamps.add(System.nanoTime());
            if (attempts.incrementAndGet() < SUCCESSFUL_ATTEMPT_NUMBER) {
                throw new RuntimeException("retryable withdrawal failure");
            }
            return null;
        };
        doAnswer(answer).when(withdrawalEventHandler).handle(anyList());

        testThriftKafkaProducer.send(
                withdrawalTopic,
                WithdrawalEventTestUtils.sinkEvent(1L, WithdrawalEventTestUtils.createdChange(1500L, null, null)));

        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> attempts.get() >= SUCCESSFUL_ATTEMPT_NUMBER);

        Awaitility.await()
                .during(2, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> attempts.get() == SUCCESSFUL_ATTEMPT_NUMBER);

        assertEquals(SUCCESSFUL_ATTEMPT_NUMBER, attemptTimestamps.size());

        long firstRetryDelayMs = Duration.ofNanos(attemptTimestamps.get(1) - attemptTimestamps.get(0)).toMillis();
        long secondRetryDelayMs = Duration.ofNanos(attemptTimestamps.get(2) - attemptTimestamps.get(1)).toMillis();

        long expectedFirstRetryDelayMs = minRetryInterval;
        long expectedSecondRetryDelayMs = Math.round(minRetryInterval * KafkaConfig.KAFKA_RETRY_BACKOFF_MULTIPLIER);

        assertTrue(
                firstRetryDelayMs >= expectedFirstRetryDelayMs - BACKOFF_TOLERANCE_MS,
                String.format(
                        "First retry delay should be at least %d ms but was %d ms",
                        expectedFirstRetryDelayMs - BACKOFF_TOLERANCE_MS,
                        firstRetryDelayMs));
        assertTrue(
                secondRetryDelayMs >= expectedSecondRetryDelayMs - BACKOFF_TOLERANCE_MS,
                String.format(
                        "Second retry delay should be at least %d ms but was %d ms",
                        expectedSecondRetryDelayMs - BACKOFF_TOLERANCE_MS,
                        secondRetryDelayMs));
        assertTrue(
                secondRetryDelayMs > firstRetryDelayMs,
                String.format(
                        "Retry delay should grow exponentially, but second=%d ms first=%d ms",
                        secondRetryDelayMs,
                        firstRetryDelayMs));
    }
}
