package uy.com.geocom.insights.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssociationRule {

    protected String[] antecedentItemIds;
    protected String consequentItemId;
    protected Double confidence;
    //Combination of confidence with another context attribute. e.g. profit
    protected Double relevance;
    protected Long rank;

}
