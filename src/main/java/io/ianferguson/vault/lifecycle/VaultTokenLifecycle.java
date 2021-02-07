package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;

// https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L49-L93
public final class VaultTokenLifecycle implements Runnable {

    private static final Logger LOG =  Logger.getLogger(VaultTokenLifecycle.class.getCanonicalName());

    private static final double GRACE_FACTOR = 0.1;
    private static final double RENEW_WAIT_PROPORTION = 2.0/3.0;

    private final Login login;
    private final Renew renew;
    private final AtomicReference<TokenWithExpiration> tokenRef;
    private final Random random;

    private final Clock clock;
    private final Sleep sleep;

    private final CountDownLatch tokenInitialized;

    VaultTokenLifecycle(Login login, Renew renew, AuthResponse token, Clock clock, Sleep sleep, Random random) {
        this.login = login;
        this.renew = renew;
        this.sleep = sleep;
        this.clock = clock;
        this.random = random;
        this.tokenInitialized = new CountDownLatch(1);

        if (token != null) {
            this.tokenRef = new AtomicReference<>(addExpiration(clock.instant(), token));
            tokenInitialized.countDown();
        } else {
            this.tokenRef = new AtomicReference<>(null);
        }
    }

    // getTokenSupplier blocks until a token is available and then return
    // a threadsafe supplier of the most recently valid Vault token
    public Supplier<AuthResponse> tokenSupplier() throws InterruptedException {
        this.tokenInitialized.await();
        return () -> {
            return tokenRef.get().token;
        };
    }

    // getTokenSupplier blocks up to timeout units long until a token is available and then return
    // a threadsafe supplier of the most recently valid Vault token
    public Supplier<AuthResponse> tokenSupplier(long timeout, TimeUnit unit) throws InterruptedException {
        this.tokenInitialized.await(timeout, unit);
        return () -> {
            return tokenRef.get().token;
        };
    }

    @Override
    public void run() {
        for (;;) {
            try {
                TokenWithExpiration tokenWithExpiration = this.tokenRef.get();

                // tokenRef will return null on the first run if the caller did no initiate the login on their own (to
                // surface errors directly at startup, for instance)
                if (tokenWithExpiration == null) {
                    LOG.fine("acquiring first vault token");
                    tokenWithExpiration = acquireStubbornly();
                    this.tokenRef.set(tokenWithExpiration);
                    tokenInitialized.countDown();
                }
                final AuthResponse token = tokenWithExpiration.token;
                Instant expiration = tokenWithExpiration.expiration;
                Duration gracePeriod = calculateGrace(Duration.between(this.clock.instant(), expiration));

                while (true) {
                    if (token.isAuthRenewable()) {
                        try {
                            final Instant now = this.clock.instant();
                            final TokenWithExpiration renewed = addExpiration(now, this.renew.renew(token));
                            expiration = renewed.expiration;
                            this.tokenRef.set(renewed);

                            // tokens renewal periods may change as they are renewed, as a result of MaxTTLs,
                            // differing initial TTL vs renewal period configurations or changes of configuration
                            // to the auth backend on the server.
                            // Because of that, we recalculate our base "grace" period any time we get a new ttl from Vault
                            gracePeriod = calculateGrace(Duration.between(now, expiration));
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

                // if renewing has stopped due to a MaxTTL being hit, or due to upstream
                // exceptions, or the underlying lease
                // hitting a MaxTTL, generate a new instance of the token/lease
                this.tokenRef.set(acquireStubbornly());
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "exception while getting or renewing vault token", e);
            }
        }
    }

    // acquireStubbornly will continually try to generate a new token lease forever
    // until it gets one
    private TokenWithExpiration acquireStubbornly() throws InterruptedException {
        ExponentialBackoff backoff = new ExponentialBackoff(sleep, Duration.ofSeconds(1), Duration.ofMinutes(8));
        for (;;) {
            try {
                final Instant now = this.clock.instant();
                return addExpiration(now, this.login.login());
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "caught exception while logging in for token, waiting " + backoff.duration().toMillis() + " ms before trying again", e);
                backoff = backoff.chill();
                continue;
            }
        }
    }

    private final class ExponentialBackoff {
        private final Duration length;
        private final Duration max;
        private final Sleep sleep;

        ExponentialBackoff(Sleep sleep, Duration length, Duration max) {
            this.sleep = sleep;
            this.length = jitter(length);
            this.max = max;
        }

        public ExponentialBackoff chill() throws InterruptedException {
            sleep.sleep(length);
            final Duration candidate = length.multipliedBy(2);
            final Duration next = (max.minus(candidate).isNegative()) ? max : candidate;
            return new ExponentialBackoff(sleep, next, max);
        }

        Duration duration() {
            return length;
        }

        // jitter all times by 0-10%
        private Duration jitter(Duration duration) {
            final long ms = duration.toMillis();
            final double jitter = ms * VaultTokenLifecycle.this.random.nextDouble() * 0.1;
            return Duration.ofMillis(ms + (long) jitter);
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

    private static final class TokenWithExpiration {
        private final AuthResponse token;
        private final Instant expiration;

        TokenWithExpiration(AuthResponse token, Instant expiration) {
            this.token = token;
            this.expiration = expiration;
        }
    }

    // now is passed as an argument to capture time before the renewal call because we pessimistically
    // assume any latency on the network call happened while the token is being sent back and should be counted againstthe TTL;
    private TokenWithExpiration addExpiration(Instant now, AuthResponse token) {
        final Duration ttl = Duration.ofSeconds(token.getAuthLeaseDuration());
        final Instant expiration = now.plus(ttl);
        return new TokenWithExpiration(token, expiration);
    }

    public static VaultTokenLifecycle loginAndCreate(LifecycleConfig config) throws VaultException {
        final AuthResponse token = config.login.login();
        return new VaultTokenLifecycle(config.login, config.renew, token, Clock.systemUTC(), new WallClockSleep(), new Random());
    }

    public static VaultTokenLifecycle create(LifecycleConfig config) {
        return new VaultTokenLifecycle(config.login, config.renew, null, Clock.systemUTC(), new WallClockSleep(), new Random());
    }

    // asDaemonThread blocks while it starts an EternalLifecycle manger on a daemon thread and will persistently
    // try to login and then renew the token. It returns a Callable that will return a thread safe Supplier that can be read
    // to get the most recently valid Vault token.
    // The Callable will return once the initial login is complete.
    public static Supplier<AuthResponse> asDaemonThread(LifecycleConfig config) throws InterruptedException {
        final VaultTokenLifecycle lifecycle = create(config);
        final Thread t = new Thread(lifecycle);
        t.setName("vault-lifecycle-daemon-"+UUID.randomUUID().toString());
        t.setDaemon(true);
        t.run();
        return lifecycle.tokenSupplier();
    }

}
