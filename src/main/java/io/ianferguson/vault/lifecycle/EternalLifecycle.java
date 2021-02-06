package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;

// https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L49-L93
public final class EternalLifecycle implements Runnable {

    private static final Random random = new Random();

    private final Login login;
    private final Renew renew;
    private final AtomicReference<AuthResponse> tokenRef;

    private final Clock clock;
    private final Sleep sleep;

    EternalLifecycle(Login login, Renew renew, AuthResponse token, Clock clock, Sleep sleep) {
        this.login = login;
        this.renew = renew;
        this.tokenRef = new AtomicReference<>(token);
        this.sleep = sleep;
        this.clock = clock;
    }

    @Override
    public void run() {
        for (;;) {
            try {
                final AuthResponse token = this.tokenRef.get();
                final Instant start = this.clock.instant();
                Duration expectedTTLPerRenewal = Duration.ofSeconds(token.getAuthLeaseDuration());
                Duration grace = calculateGrace(expectedTTLPerRenewal);
                for (;;) {

                    final Duration ttlBeforeRenewal = Duration.between(this.clock.instant(),
                            start.plus(expectedTTLPerRenewal));

                    Duration ttlAfterRenewal;
                    if (!token.isAuthRenewable()) {
                        ttlAfterRenewal = ttlBeforeRenewal;
                    } else {
                        try {
                            final AuthResponse renewed = this.renew.renew(token);
                            this.tokenRef.set(renewed);
                            ttlAfterRenewal = Duration.ofSeconds(renewed.getAuthLeaseDuration());
                        } catch (Exception e) {
                            ttlAfterRenewal = ttlBeforeRenewal;
                        }
                    }

                    // if ttlAfterRenewal is larger than the expectedTtlPerRenewal, reset the TtlPerRenewal
                    // high water mark. This heuristic is taken from golang's hashicorp/vault LifetimeWatcher,
                    //
                    // if we gain longer TTL, we give ourselves a longer grace period to sleep between renewals,
                    // but if the post renewal cycle ttl decreases, we may drop out of the renewal cycle and
                    // into the login cycle if the sleep duration would cause us to sleep past the
                    // remaining TTL less the grace period.
                    if (ttlAfterRenewal.compareTo(expectedTTLPerRenewal) > 0) {
                        final String debugMsg = "TTL after renewal (%s) was longer than expected TTL per renewal (%s) recalculating grace";
                        println(String.format(debugMsg, ttlAfterRenewal, expectedTTLPerRenewal));
                        final Duration oldGrace = grace;
                        grace = calculateGrace(ttlAfterRenewal);
                        println(String.format("Grace was (%s) and is now (%s)", oldGrace, grace));

                    }
                    expectedTTLPerRenewal = ttlAfterRenewal;

                    final double sleepNanos = (ttlAfterRenewal.toNanos() * (2.0 / 3.0));
                    final double jitteredSleepNanos = sleepNanos + (grace.toNanos() / 3.0);
                    final Duration sleepDuration = Duration.ofNanos((long) jitteredSleepNanos);

                    // if we would be inside (or past) the grace period after sleeping, skip out of
                    // the renewal
                    // loop and fall into the acquisition phase
                    final Duration ttlAfterSleep = ttlAfterRenewal.minus(sleepDuration);
                    if (ttlAfterSleep.minus(grace).isNegative()) {
                        // TKTK debug log this properly
                        final String debugMsg = "Sleep duration (%s) longer than current grace duration (%s) skipping renewal";
                        println(String.format(debugMsg, sleepDuration, grace));
                        break;
                    }

                    this.sleep.sleep(sleepDuration);
                }
            } catch (Exception e) {
                // TKTK log exception
            }
            // if renewing has stopped due to a MaxTTL being hit, or due to upstream
            // exceptions, or the underlying lease
            // hitting a MaxTTL, generate a new instance of the token/lease
            this.tokenRef.set(acquireStubbornly());
        }
    }

    // acquireStubbornly will continually try to generate a new token lease forever
    // until it gets one
    private AuthResponse acquireStubbornly() {
        for (;;) {
            try {
                return this.login.login();
            } catch (Exception innerE) {
                // TKTK log exception
                // TKTK backoff/wait
                continue;
            }
        }
    }

    // uses same logic as Hashicorp SDK
    // https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L369
    private static Duration calculateGrace(Duration ttl) {
        if (ttl.equals(Duration.ZERO)) {
            return Duration.ZERO;
        }

        final double ttlNanos = ttl.toNanos();
        final double jitterRange = 0.1 * ttlNanos;
        final double jitterGrace = random.nextDouble() * jitterRange;

        final long grace = (long) (jitterRange + jitterGrace);
        return Duration.ofNanos(grace);
    }

    public AuthResponse getToken() {
        return tokenRef.get();
    }

    private void println(String line) {
        System.out.println("[" + this.clock.instant() + "] " + line);
    }

    public static EternalLifecycle start(LifecycleConfig config) throws VaultException {
        final AuthResponse token = config.login.login();
        return new EternalLifecycle(config.login, config.renew, token, Clock.systemUTC(), new WallClockSleep());
    }

}
