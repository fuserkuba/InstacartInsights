package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Cesta de compra
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Basket {
    protected long id;
    protected LocalDateTime time;
    protected long clientId;
    protected double charge;
    protected String paymentMethod;
    protected Long posId;
}
