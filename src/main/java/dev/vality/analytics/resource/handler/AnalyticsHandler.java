package dev.vality.analytics.resource.handler;

import dev.vality.analytics.converter.*;
import dev.vality.analytics.dao.model.NamingDistribution;
import dev.vality.analytics.dao.model.*;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import dev.vality.analytics.dao.repository.clickhouse.iface.ClickHousePaymentRepository;
import dev.vality.damsel.analytics.*;
import dev.vality.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyticsHandler implements AnalyticsServiceSrv.Iface {

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;

    private final DaoNamingDistributionsToResponseConverter convertPaymentToolsToResponse;
    private final DaoErrorReasonDistributionsToResponseConverter daoErrorDistributionsToResponse;
    private final DaoErrorCodeDistributionsToResponseConverter daoErrorCodeDistributionsToResponseConverter;
    private final CostToAmountResponseConverter costToAmountResponseConverter;
    private final CountModelCountResponseConverter countModelCountResponseConverter;
    private final GroupedCurAmountToResponseConverter groupedCurAmountToResponseConverter;
    private final GroupedCurCountToResponseConverter groupedCurCountToResponseConverter;
    private final ShopAmountToResponseConverter shopAmountToResponseConverter;

    @Override
    public PaymentToolDistributionResponse getPaymentsToolDistribution(FilterRequest filterRequest) {
        log.info("-> getPaymentsToolDistribution filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        PaymentToolDistributionResponse paymentToolDistributionResponse =
                convertPaymentToolsToResponse.convert(paymentsToolDistribution);
        log.info("<- getPaymentsToolDistribution paymentToolDistributionResponse: {}", paymentToolDistributionResponse);
        return paymentToolDistributionResponse;
    }

    @Override
    public AmountResponse getPaymentsAmount(FilterRequest filterRequest) {
        log.info("-> getPaymentsAmount filterRequest: {}", filterRequest);
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convertCurrency(paymentsToolDistribution);
        log.info("<- getPaymentsAmount amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    public AmountResponse getAveragePayment(FilterRequest filterRequest) {
        log.info("-> getAveragePayment filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getAveragePayment(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convertCurrency(paymentsToolDistribution);
        log.info("<- getAveragePayment amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    public CountResponse getPaymentsCount(FilterRequest filterRequest) {
        log.info("-> getPaymentsCount filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        CountResponse countResponse = countModelCountResponseConverter.convert(paymentsToolDistribution);
        log.info("<- getPaymentsCount countResponse: {}", countResponse);
        return countResponse;
    }

    @Override
    public ErrorDistributionsResponse getPaymentsErrorDistribution(FilterRequest filterRequest) {
        log.info("-> getPaymentsErrorDistribution filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> namingDistributions = clickHousePaymentRepository.getPaymentsErrorReasonDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        ErrorDistributionsResponse errorDistributionsResponse =
                daoErrorDistributionsToResponse.convert(namingDistributions);
        log.info("<- getPaymentsErrorDistribution errorDistributionsResponse: {}", errorDistributionsResponse);
        return errorDistributionsResponse;
    }

    @Override
    public SubErrorDistributionsResponse getPaymentsSubErrorDistribution(FilterRequest filterRequest) {
        log.info("-> getPaymentsSubErrorDistribution filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> namingDistributions = clickHousePaymentRepository.getPaymentsErrorCodeDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        SubErrorDistributionsResponse subErrorDistributionsResponse =
                daoErrorCodeDistributionsToResponseConverter.convert(namingDistributions);
        log.info("<- getPaymentsSubErrorDistribution subErrorDistributionsResponse: {}", subErrorDistributionsResponse);
        return subErrorDistributionsResponse;
    }

    @Override
    public SplitAmountResponse getPaymentsSplitAmount(SplitFilterRequest splitFilterRequest) {
        log.info("-> getPaymentsSplitAmount splitFilterRequest: {}", splitFilterRequest);

        FilterRequest filterRequest = splitFilterRequest.getFilterRequest();
        SplitUnit splitUnit = splitFilterRequest.getSplitUnit();
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<SplitNumberModel> splitAmount = clickHousePaymentRepository.getPaymentsSplitAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime()),
                splitUnit
        );

        SplitAmountResponse splitAmountResponse = groupedCurAmountToResponseConverter.convert(splitAmount);
        splitAmountResponse.setResultSplitUnit(splitUnit);

        log.info("<- getPaymentsSplitCount splitAmountResponse: {}", splitAmountResponse);
        return splitAmountResponse;
    }


    @Override
    public SplitCountResponse getPaymentsSplitCount(SplitFilterRequest splitFilterRequest) {
        log.info("-> getPaymentsSplitCount splitFilterRequest: {}", splitFilterRequest);

        FilterRequest filterRequest = splitFilterRequest.getFilterRequest();
        SplitUnit splitUnit = splitFilterRequest.getSplitUnit();
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<SplitStatusNumberModel> splitAmount = clickHousePaymentRepository.getPaymentsSplitCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime()),
                splitUnit
        );

        SplitCountResponse splitCountResponse = groupedCurCountToResponseConverter.convert(splitAmount);
        splitCountResponse.setResultSplitUnit(splitUnit);

        log.info("<- getPaymentsSplitCount splitCountResponse: {}", splitCountResponse);
        return splitCountResponse;
    }

    @Override
    public AmountResponse getRefundsAmount(FilterRequest filterRequest) {
        log.info("-> getRefundsAmount filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHouseRefundRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convertCurrency(paymentsToolDistribution);

        log.info("<- getRefundsAmount amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    @Cacheable(value = "getCurrentBalances")
    public AmountResponse getCurrentBalances(MerchantFilter merchantFilter) throws TException {
        log.info("-> getCurrentBalances filterRequest: {}", merchantFilter);
        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getCurrentBalances(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds()
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convertCurrency(paymentsToolDistribution);

        log.info("<- getCurrentBalances amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    @Cacheable(value = "getCurrentShopBalances")
    public ShopAmountResponse getCurrentShopBalances(MerchantFilter merchantFilter) throws TException {
        log.info("-> getCurrentShopBalances filterRequest: {}", merchantFilter);
        List<ShopAmountModel> paymentsToolDistribution = clickHousePaymentRepository.getShopBalances(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                merchantFilter.getExcludeShopIds()
        );
        ShopAmountResponse amountResponse = shopAmountToResponseConverter.convertShop(paymentsToolDistribution);
        log.info("<- getCurrentShopBalances amountResponse: {}", amountResponse);

        return amountResponse;
    }

    @Override
    @Cacheable(value = "getCreditingsAmount")
    public AmountResponse getCreditingsAmount(FilterRequest filterRequest) throws TException {
        log.info("-> getCreditingsAmount filterRequest: {}", filterRequest);
        List<NumberModel> creditings = clickHousePaymentRepository.getCreditings(
                filterRequest.getMerchantFilter().getPartyId(),
                filterRequest.getMerchantFilter().getShopIds(),
                filterRequest.getMerchantFilter().getExcludeShopIds(),
                TypeUtil.stringToLocalDateTime(filterRequest.getTimeFilter().getFromTime()),
                TypeUtil.stringToLocalDateTime(filterRequest.getTimeFilter().getToTime())
        );
        AmountResponse amountResponse = costToAmountResponseConverter.convertCurrency(creditings);
        log.info("<- getCreditingsAmount amountResponse: {}", amountResponse);

        return amountResponse;
    }
}
