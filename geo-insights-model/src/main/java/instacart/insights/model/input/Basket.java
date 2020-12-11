package instacart.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


/**
 * Cesta de compra
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Basket {
    protected String id;
    protected LocalDateTime time;
    protected Long clientId;
    protected Double charge;
    protected String paymentMethod;
    protected Long posId;


}

