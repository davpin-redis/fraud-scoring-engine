package com.redis.fraud.signal;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.AbstractTwoRedisIT;
import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.scoring.ScoreResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Guards the constant-egress property of the signal store: the behavioural rollups are
 * fixed-width bucket hashes (+ a capped list), so the number of fields/elements a read
 * returns — and therefore the network egress per scoring call — does <b>not</b> grow with
 * the number of transactions an entity accumulates. This is the property that keeps the
 * soak-test latency flat (a hot key returns the same reply size as a quiet one).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SignalEgressIT extends AbstractTwoRedisIT {

    @Autowired
    SignalWriter writer;
    @Autowired
    @Qualifier("signalWriteConnection")
    StatefulRedisConnection<String, String> signal;

    private static final String TS = "2026-08-10T12:00:00+00:00"; // one fixed hour / 5m bucket

    private void writeN(String cid, int n) {
        for (int i = 0; i < n; i++) {
            ScoreRequest req = new ScoreRequest("egr_" + cid + "_" + i, TS, cid, "US",
                    "bene_x", "tpp_Alpha", "dev_" + cid, 100.0, "GBP", null);
            writer.updateOnce(req, new ScoreResult("approve", 0.0, null, "seed-v0"));
        }
    }

    @Test
    void bucketFieldCountIsIndependentOfEventVolume() {
        RedisCommands<String, String> sig = signal.sync();
        String quiet = "cust_egr_quiet";
        String hot = "cust_egr_hot";

        writeN(quiet, 5);
        writeN(hot, 500);

        // All events fall in the same hour and 5-min bucket → exactly one field each, for BOTH
        // entities. The hot entity has 100x the events but the SAME reply size.
        assertThat(sig.hlen(SignalKeys.velocityHour(quiet))).isEqualTo(1L);
        assertThat(sig.hlen(SignalKeys.velocityHour(hot))).isEqualTo(1L);
        assertThat(sig.hlen(SignalKeys.velocity5m(quiet))).isEqualTo(1L);
        assertThat(sig.hlen(SignalKeys.velocity5m(hot))).isEqualTo(1L);

        // The bucket's counter still reflects the true volume (the value grows, not the field count).
        assertThat(sig.hvals(SignalKeys.velocityHour(hot)).get(0)).isEqualTo("500");

        // The device surge hash is likewise a single 5-min bucket regardless of volume.
        assertThat(sig.hlen(SignalKeys.deviceSurge5m("dev_" + hot))).isEqualTo(1L);

        // The recent-events list is hard-capped, so its egress is bounded even for the hot entity.
        assertThat(sig.llen(SignalKeys.recentEvents(hot))).isEqualTo((long) SignalKeys.EVENTS_CAP);
        assertThat(sig.llen(SignalKeys.recentEvents(quiet))).isEqualTo(5L);
    }
}
