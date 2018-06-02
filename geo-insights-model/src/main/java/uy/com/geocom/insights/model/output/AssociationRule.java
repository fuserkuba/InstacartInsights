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
public class AssociationRule {
    protected Long rank;
    protected Set<Long> antecedentItemIds;
    protected Long consequentItemId;
    protected Double confidence;
    //Combination of confidence with another context attribute. e.g. profit
    protected Double relevance;

}
