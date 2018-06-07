package uy.com.geocom.insights.model.input;

import lombok.*;
import uy.com.geocom.insights.model.output.SegmentItem;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;


/**
 * Cesta de compra
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Basket{
    protected String id;
    protected LocalDateTime time;
    protected Long clientId;
    protected Double charge;
    protected String paymentMethod;
    protected Long posId;
}
