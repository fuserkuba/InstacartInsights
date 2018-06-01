package uy.com.geocom.insights.model;

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
    //TODO: Revisar lo del ordenamiento. Será útil incorporar un atributo posición o rank dentro de FrequentItemSet y AssociationRule?
    protected List<FrequentItemSet> typicalBasketsOrderedBySupport;
    protected List<AssociationRule> basketsRulesOrderedByRelevance;
}
