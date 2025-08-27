package dev.vality.analytics.dao.repository.postgres.party.management;

import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import static dev.vality.analytics.domain.db.Tables.SHOP;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.domain.db.tables.records.ShopRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.mapper.RecordRowMapper;

@Component
public class ShopDao extends AbstractGenericDao {

    private final RowMapper<Shop> shopRowMapper;

    public ShopDao(DataSource dataSource) {
        super(dataSource);
        this.shopRowMapper = new RecordRowMapper<>(SHOP, Shop.class);
    }

    public void saveShop(Shop shop) {
        ShopRecord shopRecord = getDslContext().newRecord(SHOP, shop);
        Query query = getDslContext()
                .insertInto(SHOP).set(shopRecord)
                .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
                .doUpdate()
                .set(shopRecord);
        execute(query);
    }

    public void saveShop(List<Shop> shops) {
        List<Query> queries = shops.stream()
                .map(shop -> getDslContext().newRecord(SHOP, shop))
                .map(shopRecord -> getDslContext()
                        .insertInto(SHOP).set(shopRecord)
                        .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
                        .doUpdate()
                        .set(shopRecord))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Shop getShopByPartyIdAndShopId(String partyId, String shopId) {
        Query query = getDslContext().selectFrom(SHOP)
                .where(SHOP.PARTY_ID.eq(partyId).and(SHOP.SHOP_ID.eq(shopId)));
        return fetchOne(query, shopRowMapper);
    }

    public List<Shop> getShopsByPartyIdAndContractId(String partyId, String contractId) {
        Query query = getDslContext().selectFrom(SHOP)
                .where(SHOP.PARTY_ID.eq(partyId)
                        .and(SHOP.CONTRACT_ID.eq(contractId)));
        return fetch(query, shopRowMapper);
    }

    public void removeShop(Shop shop) {
        Query query = getDslContext().update(SHOP)
                .set(SHOP.DELETED, true)
                .set(SHOP.VERSION_ID, shop.getVersionId())
                .set(SHOP.CHANGED_BY_ID, shop.getChangedById())
                .set(SHOP.CHANGED_BY_NAME, shop.getChangedByName())
                .set(SHOP.CHANGED_BY_EMAIL, shop.getChangedByEmail())
                .where(SHOP.PARTY_ID.eq(shop.getPartyId())
                        .and(SHOP.SHOP_ID.eq(shop.getShopId())));
        execute(query);
    }

}
