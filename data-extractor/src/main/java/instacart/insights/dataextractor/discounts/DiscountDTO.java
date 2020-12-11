package instacart.insights.dataextractor.discounts;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class DiscountDTO {

    private LocalDateTime date;

    private String pos;

    private String local;

    private String ticketNumber;

    private double amount;

    private String article;

    private int quantity;

    private String promotion;

    @Override
    public String toString() {
        return date + "," + pos + "," + local + "," + ticketNumber + "," + promotion + "," + article + "," + quantity + "," + amount;
    }
}
