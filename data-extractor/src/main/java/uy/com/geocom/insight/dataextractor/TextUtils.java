package uy.com.geocom.insight.dataextractor;

public class TextUtils {

    public static String sanitize(String text) {
        return text.trim().replace(",", ".");
    }
}
