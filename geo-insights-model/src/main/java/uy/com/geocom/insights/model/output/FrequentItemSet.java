package uy.com.geocom.insights.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FrequentItemSet {
    protected Long rank;
    protected Set<Long> itemIds;
    protected Double support;
}
