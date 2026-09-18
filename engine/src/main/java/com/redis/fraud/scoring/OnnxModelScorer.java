package com.redis.fraud.scoring;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.redis.fraud.feature.FeatureVector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * In-process ONNX scorer for the Track-A feedback model (design doc §8.4.3, Phase 4).
 * Loads a versioned {@code .onnx} artifact (exported by {@code pipeline/export_onnx.py})
 * plus its sidecar manifest ({@code <model>.manifest.json}: version + ordered feature
 * contract), builds the model's input vector from the assembled {@link FeatureVector} in
 * that exact order, and returns the fraud probability.
 *
 * <p>Hot-reloads on a {@code model:invalidate} Pub/Sub message (mirrors {@code cfg:invalidate},
 * §7.2): a retrained/promoted model is picked up by overwriting the artifact and publishing,
 * with no redeploy. The {@code (version, feature-order)} are read from the manifest so a
 * reloaded model brings its own contract.
 *
 * <p>Active only when {@code fraud.model.enabled=true} and {@code fraud.model.type=onnx}.
 */
@Component
@ConditionalOnExpression("${fraud.model.enabled:false} and '${fraud.model.type:linear}' == 'onnx'")
public class OnnxModelScorer implements ModelScorer {

    private static final Logger log = LoggerFactory.getLogger(OnnxModelScorer.class);
    static final String INVALIDATE_CHANNEL = "model:invalidate";

    private final OrtEnvironment env = OrtEnvironment.getEnvironment();
    private final ObjectMapper mapper;
    private final String modelPath;

    private volatile Loaded loaded;   // atomically swapped on reload

    private record Loaded(OrtSession session, String inputName, List<String> featureOrder, String version) {
    }

    /** Spring bean: load from config path and subscribe to model:invalidate for hot-reload. */
    public OnnxModelScorer(ObjectMapper mapper,
                           @Value("${fraud.model.path}") String modelPath,
                           @Qualifier("redisClient") RedisClient redisClient) {
        this.mapper = mapper;
        this.modelPath = modelPath;
        reload(modelPath);
        subscribeInvalidations(redisClient);
    }

    /** Test constructor: load only, no Pub/Sub. */
    OnnxModelScorer(String modelPath, ObjectMapper mapper) {
        this.mapper = mapper;
        this.modelPath = modelPath;
        reload(modelPath);
    }

    @Override
    public double fraudProbability(FeatureVector features) {
        Loaded m = loaded;
        float[][] input = new float[1][m.featureOrder.size()];
        for (int i = 0; i < m.featureOrder.size(); i++) {
            Double v = features.asDouble(m.featureOrder.get(i));
            input[0][i] = v == null ? 0.0f : v.floatValue();
        }
        try (OnnxTensor tensor = OnnxTensor.createTensor(env, input);
             OrtSession.Result result = m.session.run(Map.of(m.inputName, tensor))) {
            float[][] probs = (float[][]) result.get("probabilities")
                    .orElseThrow(() -> new IllegalStateException("no 'probabilities' output"))
                    .getValue();
            return probs[0][1];   // P(fraud)
        } catch (Exception e) {
            throw new IllegalStateException("ONNX scoring failed", e);
        }
    }

    @Override
    public String version() {
        return loaded.version;
    }

    /** (Re)load the artifact + manifest from the given path and atomically swap. */
    public final void reload(String path) {
        try {
            byte[] onnx = Files.readAllBytes(Path.of(path));
            OrtSession session = env.createSession(onnx, new OrtSession.SessionOptions());
            String inputName = session.getInputNames().iterator().next();
            Manifest man = readManifest(path);
            Loaded old = this.loaded;
            this.loaded = new Loaded(session, inputName, man.features(), man.version());
            if (old != null) {
                old.session().close();
            }
            log.info("ONNX model loaded: version={} features={} input='{}' from {}",
                    man.version(), man.features().size(), inputName, path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load ONNX model from " + path, e);
        }
    }

    private Manifest readManifest(String modelPath) {
        String manifestPath = modelPath.replaceAll("\\.onnx$", "") + ".manifest.json";
        try {
            return mapper.readValue(Files.readString(Path.of(manifestPath)), Manifest.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read model manifest " + manifestPath, e);
        }
    }

    private void subscribeInvalidations(RedisClient redisClient) {
        try {
            StatefulRedisPubSubConnection<String, String> pubSub = redisClient.connectPubSub();
            pubSub.addListener(new RedisPubSubAdapter<>() {
                @Override
                public void message(String channel, String msg) {
                    log.info("model:invalidate received; reloading {}", modelPath);
                    try {
                        reload(modelPath);
                    } catch (RuntimeException e) {
                        log.error("Model reload failed; keeping current model: {}", e.toString());
                    }
                }
            });
            pubSub.async().subscribe(INVALIDATE_CHANNEL);
            log.info("Subscribed to model invalidation channel '{}'", INVALIDATE_CHANNEL);
        } catch (RuntimeException e) {
            log.warn("Could not subscribe to {}: {}", INVALIDATE_CHANNEL, e.getMessage());
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Manifest(String version, List<String> features) {
        Manifest {
            features = features == null ? List.of() : Collections.unmodifiableList(features);
        }
    }
}
