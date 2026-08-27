package com.redis.fraud.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A configurable time window (design doc §6.1), stored as JSON in the
 * {@code cfg:windows} hash. Field names map from the snake_case JSON via the
 * SNAKE_CASE Jackson strategy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WindowDef(
        String windowId,
        long lookbackStartOffsetSec,
        long lookbackEndOffsetSec,
        String bucketResolution,
        String tier,
        long retentionSec
) {
}
