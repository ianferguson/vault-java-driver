package io.ianferguson.vault.lifecycle;

import java.time.Duration;

final class ExponentialBackoff {
    /**
     * 
     */
    private final VaultTokenLifecycle vaultTokenLifecycle;
    private final Duration length;
    private final Duration max;
    private final Sleep sleep;

    ExponentialBackoff(VaultTokenLifecycle vaultTokenLifecycle, Sleep sleep, Duration length, Duration max) {
        this.vaultTokenLifecycle = vaultTokenLifecycle;
        this.sleep = sleep;
        this.length = jitter(length);
        this.max = max;
    }

    public ExponentialBackoff chill() throws InterruptedException {
        sleep.sleep(length);
        final Duration candidate = length.multipliedBy(2);
        final Duration next = (max.minus(candidate).isNegative()) ? max : candidate;
        return new ExponentialBackoff(this.vaultTokenLifecycle, sleep, next, max);
    }

    Duration duration() {
        return length;
    }

    // jitter all times by 0-10%
    private Duration jitter(Duration duration) {
        final long ms = duration.toMillis();
        final double jitter = ms * this.vaultTokenLifecycle.random.nextDouble() * 0.1;
        return Duration.ofMillis(ms + (long) jitter);
    }

}