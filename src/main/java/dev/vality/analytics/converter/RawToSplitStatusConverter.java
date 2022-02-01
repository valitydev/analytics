package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.SplitStatusNumberModel;
import dev.vality.analytics.dao.utils.SplitUtils;
import dev.vality.damsel.analytics.SplitUnit;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.Map;

@Component
public class RawToSplitStatusConverter {

    public static final String NUM = "num";
    public static final String CURRENCY = "currency";
    public static final String STATUS = "status";

    public SplitStatusNumberModel convert(Map row, SplitUnit splitUnit) {
        SplitStatusNumberModel splitStatusNumberModel = new SplitStatusNumberModel();
        if (row.get(NUM) instanceof Double) {
            splitStatusNumberModel.setNumber((((Double) row.get(NUM)).longValue()));
        }
        if (row.get(NUM) instanceof Long) {
            splitStatusNumberModel.setNumber((Long) row.get(NUM));
        } else {
            splitStatusNumberModel.setNumber(((BigInteger) row.get(NUM)).longValue());
        }
        splitStatusNumberModel.setOffset(SplitUtils.generateOffset(row, splitUnit));
        splitStatusNumberModel.setCurrency((String) row.get(CURRENCY));
        splitStatusNumberModel.setStatus((String) row.get(STATUS));
        return splitStatusNumberModel;
    }

}
