package com.redis.fraud.money;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Stubbed FX normalization at ingest (design doc §3.2): converts a transaction's
 * native amount into the configured base currency before it feeds features and
 * aggregates (§7.4). All fixture data is GBP, so in practice this is a 1.0
 * conversion; the static table stands in for a real, periodically-refreshed FX
 * source. Unknown currencies pass through unconverted (logged as a gap in a real
 * system).
 */
@Service
public class FxService {

    private final String baseCurrency;
    // Illustrative static rates -> base (GBP). A real implementation refreshes these.
    private static final Map<String, Double> RATES_TO_GBP = Map.of(
            "GBP", 1.0, "USD", 0.79, "EUR", 0.86, "SGD", 0.58, "BRL", 0.16);

    public FxService(@Value("${fraud.base-currency:GBP}") String baseCurrency) {
        this.baseCurrency = baseCurrency;
    }

    public String baseCurrency() {
        return baseCurrency;
    }

    /** Convert {@code amount} in {@code currency} to the base currency. */
    public double toBase(double amount, String currency) {
        if (currency == null || currency.equalsIgnoreCase(baseCurrency)) {
            return amount;
        }
        Double rate = RATES_TO_GBP.get(currency.toUpperCase());
        return rate == null ? amount : amount * rate;
    }
}
