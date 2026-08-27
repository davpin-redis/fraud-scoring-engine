package com.redis.fraud.scoring;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.redis.fraud.feature.FeatureVector;
import java.io.InputStream;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Embedded logistic-regression scorer (design doc §8.2). Loads standardization
 * stats and weights trained offline by {@code ml/train.py} from
 * {@code /model/model.json}, extracts features in the trained order, standardizes,
 * and returns a sigmoid probability.
 *
 * <p>Only created when {@code fraud.model.enabled=true}; when disabled this bean
 * is absent and scoring bypasses the model entirely (seed-v0 rules-only).
 */
@Component
@ConditionalOnProperty(prefix = "fraud.model", name = "enabled", havingValue = "true")
public class LinearModelScorer implements ModelScorer {

    private static final String MODEL_RESOURCE = "/model/model.json";

    private final ModelParams params;

    public LinearModelScorer(ObjectMapper mapper) {
        try (InputStream in = getClass().getResourceAsStream(MODEL_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Model artifact not found on classpath: " + MODEL_RESOURCE);
            }
            this.params = mapper.readValue(in, ModelParams.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load model artifact " + MODEL_RESOURCE, e);
        }
    }

    @Override
    public double fraudProbability(FeatureVector features) {
        List<String> order = params.featureOrder();
        double z = params.intercept();
        for (int i = 0; i < order.size(); i++) {
            double raw = extract(features, order.get(i));
            double std = params.stds().get(i);
            double standardized = (raw - params.means().get(i)) / (std == 0.0 ? 1.0 : std);
            z += params.weights().get(i) * standardized;
        }
        return sigmoid(z);
    }

    @Override
    public String version() {
        return params.version();
    }

    private static double extract(FeatureVector fv, String name) {
        if ("cross_border".equals(name)) {
            Object customer = fv.get("customer_portfolio_country");
            Object beneficiary = fv.get("beneficiary_country");
            return beneficiary != null && !beneficiary.equals(customer) ? 1.0 : 0.0;
        }
        Object v = fv.get(name);
        return v instanceof Number n ? n.doubleValue() : 0.0;
    }

    private static double sigmoid(double z) {
        if (z < -60) {
            return 0.0;
        }
        if (z > 60) {
            return 1.0;
        }
        return 1.0 / (1.0 + Math.exp(-z));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ModelParams(
            String version,
            List<String> featureOrder,
            List<Double> means,
            List<Double> stds,
            List<Double> weights,
            double intercept
    ) {
    }
}
