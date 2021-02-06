package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;

// https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L49-L93
public final class EternalLifecycle implements Runnable {

    private static final Logger LOG =  Logger.getLogger(EternalLifecycle.class.getCanonicalName());

    private static final double GRACE_FACTOR = 0.1;
    private static final double RENEW_WAIT_PROPORTION = 3.0/5.0;

    private final Login login;
    private final Renew renew;
    private final AtomicReference<AuthResponse> tokenRef;
    private final Random random = new Random();

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
                final Duration remaining = Duration.ofSeconds(token.getAuthLeaseDuration());
                final Duration gracePeriod = calculateGrace(remaining);
                Instant expiration = this.clock.instant().plus(remaining);
                while (true) {
                    if (token.isAuthRenewable()) {
                        try {
                            final AuthResponse renewed = this.renew.renew(token);
                            this.tokenRef.set(renewed);
                            final Duration newTTL = Duration.ofSeconds(renewed.getAuthLeaseDuration());
                            expiration = this.clock.instant().plus(newTTL);
                        } catch (Exception e) {
                            LOG.log(Level.WARNING, "caught exception while renewing token", e);
                            // fall through and sleep for a while longer if we can
                        }
                    }

                    // our internal deadline for renewal is the actual expiration, minus the grace period
                    // we sleep for shorter and shorter periods as that internal deadline approaches, but never
                    // for less than 1/4 of the grace period
                    //
                    // For a 1 hour TTL token, this means that we'll have an internal dead of renewing by
                    // around 48-54 minutes (60 minutes - (60 minutes * 0.1 grace factor)),
                    // and will make attempts no more frequently than once 1.5-3 minutes (6-12 minutes/4)
                    final Instant renewalDeadline = expiration.minus(gracePeriod);
                    final Instant momentAfterRenew = this.clock.instant();

                    final Duration ttl = Duration.between(momentAfterRenew, renewalDeadline);
                    final double sleepNanos = ttl.toNanos() * RENEW_WAIT_PROPORTION;
                    final double jitteredSleepNanos = sleepNanos + (gracePeriod.toNanos() / 4.0);
                    final Duration sleepDuration = Duration.ofNanos((long) jitteredSleepNanos);

                    // if we would be inside (or past) the grace period after sleeping, skip out of
                    // the renewal loop and fall into the acquisition phase
                    final Instant estimatedWake = momentAfterRenew.plus(sleepDuration);
                    if (estimatedWake.isAfter(renewalDeadline)) {
                        final String debugMsg = "[FAILURE] exiting renewal loop: estimated wake up in %s seconds is after renewal deadline in %s seconds";
                        LOG.finest(() -> String.format(debugMsg, sleepDuration.getSeconds(), Duration.between(momentAfterRenew, renewalDeadline).getSeconds()));
                        break;
                    }

                    this.sleep.sleep(sleepDuration);
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "caught exception durin token renewal loop", e);
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
            } catch (Exception e) {
                LOG.log(Level.WARNING, "caught exception while logging in for token", e);
                // TKTK properly handle interrupted exception here
                // TKTK exponential backoff/wait
                continue;
            }
        }
    }

    // uses same logic as Hashicorp SDK
    // https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L369
    // Roughly the "grace period" is 10-20% of the lease's initial observable TTL, and any renewals that are schedule
    // within a grace periods length of the token expiration are skipped, and a login is attempted instead.
    private Duration calculateGrace(Duration ttl) {
        if (ttl.equals(Duration.ZERO)) {
            return Duration.ZERO;
        }

        final double ttlNanos = ttl.toNanos();
        final double jitterRange = GRACE_FACTOR * ttlNanos;
        final double jitterGrace = random.nextDouble() * jitterRange;

        final long grace = (long) (jitterRange + jitterGrace);
        return Duration.ofNanos(grace);
    }

    public AuthResponse getToken() {
        return tokenRef.get();
    }

    public static EternalLifecycle start(LifecycleConfig config) throws VaultException {
        final AuthResponse token = config.login.login();
        return new EternalLifecycle(config.login, config.renew, token, Clock.systemUTC(), new WallClockSleep());
    }

}
