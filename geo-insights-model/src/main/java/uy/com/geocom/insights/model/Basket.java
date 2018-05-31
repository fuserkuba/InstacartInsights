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
    protected List<Long> purchaseId;
    protected Long posId;
    protected long charge;
    protected String paymentMethod;
}
