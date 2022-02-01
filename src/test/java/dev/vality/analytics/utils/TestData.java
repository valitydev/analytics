package dev.vality.analytics.utils;

import dev.vality.damsel.domain.*;
import dev.vality.damsel.domain_config.*;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TestData {

    public static Commit buildInsertCategoryCommit(Integer id, String name, String description,
                                                   CategoryType categoryType) {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        return new Commit(List.of(operation));
    }

    public static Commit buildUpdateCategoryCommit(Integer id,
                                                   String name,
                                                   String description,
                                                   CategoryType categoryType,
                                                   DomainObject oldObject) {
        UpdateOp updateOp = new UpdateOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        updateOp.setNewObject(domainObject);
        updateOp.setOldObject(oldObject);
        Operation operation = new Operation();
        operation.setUpdate(updateOp);

        return new Commit(List.of(operation));
    }

    public static Commit buildRemoveCategoryCommit(Integer id, String name, String description,
                                                   CategoryType categoryType) {
        Operation operation = new Operation();
        RemoveOp removeOp = new RemoveOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        removeOp.setObject(domainObject);
        operation.setRemove(removeOp);

        return new Commit(List.of(operation));
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

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

}
