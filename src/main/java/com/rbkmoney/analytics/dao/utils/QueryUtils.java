package com.rbkmoney.analytics.dao.utils;

import java.util.List;

public class QueryUtils {

    public static StringBuilder generateInList(List<String> shopIds) {
        StringBuilder inList = null;
        for (String shopId : shopIds) {
            if (inList == null) {
                inList = new StringBuilder();
                inList.append("in ( ?");
            } else {
                inList.append(",?");
            }
        }
        if (inList != null) {
            inList.append(") ");
        }
        return inList;
    }

}
