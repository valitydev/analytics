package dev.vality.analytics.listener.handler.rate;

import dev.vality.analytics.dao.repository.postgres.RateDao;
import dev.vality.analytics.listener.mapper.rate.CurrencyEventMapper;
import dev.vality.exrates.events.CurrencyEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CurrencyEventHandlerTest {

    @Mock
    private CurrencyEventMapper currencyEventMapper;
    @Mock
    private RateDao rateDao;

    @Test
    public void handleShouldPropagateFailureWithoutAcknowledging() {
        CurrencyEventHandler handler = new CurrencyEventHandler(currencyEventMapper, rateDao);
        CurrencyEvent event = mock(CurrencyEvent.class);
        TrackingAcknowledgment acknowledgment = new TrackingAcknowledgment();
        RuntimeException expected = new RuntimeException("rate failure");
        when(currencyEventMapper.map(event)).thenThrow(expected);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> handler.handle(List.of(event), acknowledgment));

        assertSame(expected, actual);
        assertFalse(acknowledgment.isAcknowledged());
        verifyNoInteractions(rateDao);
    }

    private static final class TrackingAcknowledgment implements Acknowledgment {

        private boolean acknowledged;

        @Override
        public void acknowledge() {
            acknowledged = true;
        }

        private boolean isAcknowledged() {
            return acknowledged;
        }
    }
}
