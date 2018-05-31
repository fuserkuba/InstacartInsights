package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Lugar donde se registró la venta
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PointOfSale {
    protected String name;
    protected String location;
    protected List<Basket> baskets;
}
