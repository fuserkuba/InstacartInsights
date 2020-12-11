package instacart.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Relación entre la cesta y el producto adquirido
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Purchase {
    protected String productId;
    protected String basketId;
    protected Double price;
    protected Integer units;
    //Contexto de la compra: promoción, descuentos, navidades
    protected String context;
}
