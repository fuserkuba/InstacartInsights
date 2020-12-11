package instacart.insights.dataextractor.ticketitem;

import lombok.Data;

@Data
public class TicketItemDTO {
    private String id;
    private double amount;
    private int quantity;
    private String article;
    private String type;

    @Override
    public String toString() {
        return id + "," + amount + "," + quantity + "," + article + "," + type;
    }
}
