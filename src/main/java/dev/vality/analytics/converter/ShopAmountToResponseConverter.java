package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.ShopAmountModel;
import dev.vality.damsel.analytics.ShopAmountResponse;
import dev.vality.damsel.analytics.ShopGroupedAmount;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Component
public class ShopAmountToResponseConverter {

    public ShopAmountResponse convertShop(List<ShopAmountModel> shopAmountModels) {
        List<ShopGroupedAmount> collect = shopAmountModels.stream()
                .map(cost -> new ShopGroupedAmount()
                        .setAmount(cost.getAmount())
                        .setShopId(cost.getShopId())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new ShopAmountResponse().setGroupsAmount(collect);
    }

}
