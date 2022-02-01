package dev.vality.analytics.dao.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class SplitNumberModel extends NumberModel {

    private Long offset;

}
