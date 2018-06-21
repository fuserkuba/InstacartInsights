package uy.com.geocom.insights.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FrequentItemSet {
    protected LinkedHashSet<String> items;
    protected Long freq;
    protected Long rank;
}
