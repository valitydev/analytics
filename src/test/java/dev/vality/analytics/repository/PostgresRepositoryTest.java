package dev.vality.analytics.repository;

import dev.vality.analytics.config.PostgresqlTest;
import dev.vality.analytics.dao.model.AdjustmentRow;
import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.dao.model.RefundRow;
import dev.vality.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@PostgresqlTest
public class PostgresRepositoryTest {

    @Autowired
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    @Autowired
    private PartyDao partyDao;

    @Autowired
    private ShopDao shopDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void testCount() {
        postgresBalanceChangesRepository.insertPayments(List.of(payment()));
        postgresBalanceChangesRepository.insertRefunds(List.of(refund()));
        postgresBalanceChangesRepository.insertAdjustments(List.of(adjustment()));

        long count = postgresJdbcTemplate.queryForObject(
                "SELECT count(*) AS count FROM analytics.balance_change",
                (resultSet, i) -> resultSet.getLong("count"));

        assertEquals(3L, count);
    }

    @Test
    public void testPartySave() {
        Party party = random(Party.class);
        partyDao.saveParty(party);
        Party savedParty = partyDao.getPartyById(party.getPartyId());
        Assertions.assertEquals(party, savedParty);
    }

    @Test
    public void testShopSave() {
        Shop shop = random(Shop.class);
        shopDao.saveShop(shop);
        Shop savedShop = shopDao.getShopByPartyIdAndShopId(shop.getPartyId(), shop.getShopId());
        Assertions.assertEquals(shop, savedShop);
    }

    @Test
    public void testDuplicatePartySave() {
        Party firstParty = random(Party.class);
        partyDao.saveParty(firstParty);
        Party secondParty = random(Party.class);
        secondParty.setPartyId(firstParty.getPartyId());
        partyDao.saveParty(secondParty);
        Party savedParty = partyDao.getPartyById(secondParty.getPartyId());
        Assertions.assertEquals(secondParty, savedParty);
    }

    @Test
    public void testDuplicateShopSave() {
        Shop firstShop = random(Shop.class);
        shopDao.saveShop(firstShop);
        Shop secondShop = random(Shop.class);
        secondShop.setPartyId(firstShop.getPartyId());
        secondShop.setShopId(firstShop.getShopId());
        shopDao.saveShop(secondShop);
        Shop savedShop = shopDao.getShopByPartyIdAndShopId(secondShop.getPartyId(), secondShop.getShopId());
        Assertions.assertEquals(secondShop, savedShop);
    }

    private PaymentRow payment() {
        PaymentRow paymentRow = new PaymentRow();
        paymentRow.setInvoiceId("invoice_id");
        paymentRow.setSequenceId(1L);
        paymentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        paymentRow.setCurrency("RUB");
        paymentRow.setPartyId("party_id");
        paymentRow.setShopId("shop_id");
        paymentRow.setCashFlowResult(CashFlowResult.builder()
                .amount(1000L)
                .systemFee(100L)
                .build());
        return paymentRow;
    }

    private RefundRow refund() {
        RefundRow refundRow = new RefundRow();
        refundRow.setInvoiceId("invoice_id");
        refundRow.setSequenceId(2L);
        refundRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        refundRow.setCurrency("RUB");
        refundRow.setPartyId("party_id");
        refundRow.setShopId("shop_id");
        refundRow.setCashFlowResult(CashFlowResult.builder()
                .amount(500L)
                .systemFee(50L)
                .build());
        return refundRow;
    }

    private AdjustmentRow adjustment() {
        AdjustmentRow adjustmentRow = new AdjustmentRow();
        adjustmentRow.setInvoiceId("invoice_id");
        adjustmentRow.setSequenceId(3L);
        adjustmentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        adjustmentRow.setCurrency("RUB");
        adjustmentRow.setPartyId("party_id");
        adjustmentRow.setShopId("shop_id");
        adjustmentRow.setCashFlowResult(CashFlowResult.builder()
                .systemFee(250L)
                .build());
        adjustmentRow.setOldCashFlowResult(CashFlowResult.builder()
                .systemFee(100L)
                .build());
        return adjustmentRow;
    }
}
