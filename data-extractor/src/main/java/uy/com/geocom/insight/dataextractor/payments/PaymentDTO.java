package uy.com.geocom.insight.dataextractor.payments;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Builder
@Data
public class PaymentDTO {

    private LocalDateTime date;

    private String pos;

    private String local;

    private String ticketNumber;

    private double amount;

    private double affectedAmount;

    private String paymentMode;

    private boolean isChange;

    private String client;

    @Override
    public String toString() {
        return date + "," + pos + "," + local + "," + ticketNumber + "," + amount + "," + affectedAmount + "," + paymentMode + "," + isChange + "," + client;
    }
}
