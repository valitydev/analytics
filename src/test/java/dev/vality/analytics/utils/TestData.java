package dev.vality.analytics.utils;

import dev.vality.damsel.domain.*;
import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalInsertOp;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.damsel.domain_config_v2.RemoveOp;
import dev.vality.damsel.domain_config_v2.UpdateOp;

import java.util.Set;
import java.util.UUID;

public class TestData {

    public static FinalOperation buildInsertCategoryOperation(Integer id, String name, String description,
                                                              CategoryType categoryType) {
        FinalOperation operation = new FinalOperation();
        FinalInsertOp insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        return operation;
    }

    public static FinalOperation buildUpdateCategoryOperation(Integer id,
                                                              String name,
                                                              String description,
                                                              CategoryType categoryType) {
        UpdateOp updateOp = new UpdateOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        updateOp.setObject(domainObject);
        FinalOperation operation = new FinalOperation();
        operation.setUpdate(updateOp);

        return operation;
    }

    public static FinalOperation buildRemoveCategoryOperation(Integer id) {
        RemoveOp removeOp = new RemoveOp();
        Reference reference = new Reference();
        CategoryRef categoryRef = new CategoryRef();
        categoryRef.setId(id);
        reference.setCategory(categoryRef);
        removeOp.setRef(reference);
        FinalOperation operation = new FinalOperation();
        operation.setRemove(removeOp);

        return operation;
    }

    public static CategoryObject buildCategoryObject(Integer id, String name, String description,
                                                     CategoryType categoryType) {
        Category category = new Category();
        category.setName(name);
        category.setDescription(description);
        category.setType(categoryType);

        CategoryObject categoryObject = new CategoryObject();
        categoryObject.setData(category);
        CategoryRef categoryRef = new CategoryRef();
        categoryRef.setId(id);
        categoryObject.setRef(categoryRef);

        return categoryObject;
    }

    public static CountryObject buildCountryObject() {
        Country country = new Country();
        country.setName(randomString());
        country.setTradeBlocs(Set.of(new TradeBlocRef().setId(randomString())));
        CountryObject countryObject = new CountryObject();
        countryObject.setData(country);
        countryObject.setRef(new CountryRef().setId(CountryCode.ABH));
        return countryObject;
    }

    public static TradeBlocObject buildTradeBlocObject() {
        TradeBloc tradeBloc = new TradeBloc();
        tradeBloc.setName(randomString());
        tradeBloc.setDescription(randomString());
        TradeBlocObject tradeBlocObject = new TradeBlocObject();
        tradeBlocObject.setData(tradeBloc);
        tradeBlocObject.setRef(new TradeBlocRef().setId(randomString()));
        return tradeBlocObject;
    }

    public static Author buildAuthor() {
        Author author = new Author();
        author.setEmail(randomString());
        author.setName(randomString());
        author.setId(randomString());
        return author;
    }

    public static HistoricalCommit buildHistoricalCommit() {
        HistoricalCommit historicalCommit = new HistoricalCommit();
        historicalCommit.setVersion(1L);
        historicalCommit.setCreatedAt("2016-03-22T06:12:27Z");
        historicalCommit.setChangedBy(buildAuthor());
        return historicalCommit;
    }

    public static PartyConfigObject buildPartyConfigObject() {
        PartyConfig partyConfig = new PartyConfig();
        PartyContactInfo contactInfo = new PartyContactInfo();
        contactInfo.setRegistrationEmail(randomString() + "@example.com");
        partyConfig.setContactInfo(contactInfo);

        PartyConfigObject partyConfigObject = new PartyConfigObject();
        partyConfigObject.setData(partyConfig);
        PartyConfigRef partyConfigRef = new PartyConfigRef();
        partyConfigRef.setId(randomString());
        partyConfigObject.setRef(partyConfigRef);
        return partyConfigObject;
    }

    public static ShopConfigObject buildShopConfigObject() {
        ShopConfig shopConfig = new ShopConfig();
        shopConfig.setName(randomString());
        shopConfig.setDescription(randomString());

        PartyConfigRef partyConfigRef = new PartyConfigRef();
        partyConfigRef.setId(randomString());
        shopConfig.setPartyRef(partyConfigRef);

        ShopConfigObject shopConfigObject = new ShopConfigObject();
        shopConfigObject.setData(shopConfig);
        ShopConfigRef shopConfigRef = new ShopConfigRef();
        shopConfigRef.setId(randomString());
        shopConfigObject.setRef(shopConfigRef);
        return shopConfigObject;
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

}
