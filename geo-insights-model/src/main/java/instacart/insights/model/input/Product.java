package instacart.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {
    protected String id;
    protected String name;
    protected String category;
    protected String department;
    protected String type;
    protected String brand;
    protected Double profit;
}
