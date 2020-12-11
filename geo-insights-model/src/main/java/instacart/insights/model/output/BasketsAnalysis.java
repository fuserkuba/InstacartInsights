package instacart.insights.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BasketsAnalysis extends Insight {
    protected List<FrequentItemSet> typicalBasketsOrderedBySupport;
    protected List<AssociationRule> basketsRulesOrderedByRelevance;
}
