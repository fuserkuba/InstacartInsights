package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {
    protected String name;
    protected String category;
    protected String department;
    protected String type;
    protected String brand;
    protected long profit;
}
