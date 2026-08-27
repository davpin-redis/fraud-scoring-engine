package com.redis.fraud.scoring;

import com.redis.fraud.feature.FeatureVector;

/**
 * In-process ML scorer (design doc §8.2, §3.6): returns a calibrated fraud
 * probability in [0, 1] for a feature vector. Implementations are embedded in
 * the scoring process — no network hop. Only instantiated when the model is
 * enabled (see {@code fraud.model.enabled}); a future ONNX Runtime scorer can
 * drop in behind this same interface.
 */
public interface ModelScorer {

    double fraudProbability(FeatureVector features);

    String version();
}
