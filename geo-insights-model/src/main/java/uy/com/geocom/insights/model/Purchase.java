package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Relación entre la cesta y el producto adquirido
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Purchase {
    protected Product product;
    protected Basket basket;
    protected long price;
    protected long units;
    //Contexto de la compra: promoción, descuentos, navidades
    protected String context;
}
