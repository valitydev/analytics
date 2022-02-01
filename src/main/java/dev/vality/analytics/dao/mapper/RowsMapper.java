package dev.vality.analytics.dao.mapper;

import java.util.List;
import java.util.Map;

public interface RowsMapper<T> {

    List<T> map(List<Map<String, Object>> rows);

}
