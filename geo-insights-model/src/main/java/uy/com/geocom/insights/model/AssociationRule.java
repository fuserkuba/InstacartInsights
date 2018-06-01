package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssociationRule {
    protected Set<Long> antecedentItemIds;
    protected long consequentItemId;
    protected double confidence;
    //Combination of confidence with another context attribute. e.g. profit
    protected double relevance;
}
