package instacart.insights.dataextractor;

public class TextUtils {

    public static String sanitize(String text) {
        return text.trim().replace(",", ".");
    }
}
