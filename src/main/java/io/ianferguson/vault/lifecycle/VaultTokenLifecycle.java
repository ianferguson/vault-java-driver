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

import io.ianferguson.vault.response.AuthResponse;

/**
 * EXPERIMENTAL! use at your own risk, this is an alpha testing/experimental package that may include breaking changes
 * in minor version.
 *
 * VaultTokenLifecycle manages the login and renewal of vault tokens for an application indefintely. somewhat based on:
 * https://github.com/hashicorp/vault/blob/b2927012ba9131f68606debec13bfc221b221912/vendor/github.com/hashicorp/vault/api/lifetime_watcher.go#L49-L93
 *
 * Usage:
 * ```
 * final VaultConfig config = new VaultConfig();
 * final Vault vault = new Vault(config);
 *
 * final Supplier<AuthResponse> tokens = VaultTokenLifecycle,asDaemonThread()
 * config.token(tokens.get().getAuthClientToken());
 * ```
 * TKTK: alternatively, this could be wired to update a VaultConfig object in place, since the current
 * Vault objects embed the mutable VaultConfig objects they take in place (though it would require marking some things
 * volatile) -- that might make a better user experience, as the use could potentially get a single vault client
 * and have the tokens auto updated in the background rather than needing to create a new client or config per call.
 *
 * Perhaps this can be done flexibly by allowing things to register Consumer<AuthResponse> as call backs that are
 * called whenever a token is renewed or logged in -- the internal tokenRef.set call could even just be a callback
 * at that point as well:
 *
 *
    private static final class UpdateTokenOnChange implements Consumer<AuthResponse> {

        private final VaultConfig config;

        UpdateTokenOnChange(VaultConfig config) {
            this.config = config;
        }

        @Override
        public void accept(AuthResponse t) {
            config.token(t.getAuthClientToken())
        }

    }
 *
 * and then the user would do:
 * final VaultConfig config = new VaultConfig();
 * lifecycle.register(UpdateTokenOnChange(config));
 * final Vault vault = new Vault(config);
 *
 * there is yet another option of having this lifecycle watcher take a vault config and init clients itself, but
 * I think the callbacks approach probably is the right balance for now
 *
 */
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

    VaultTokenLifecycle(Login login, Renew renew, Clock clock, Sleep sleep, Random random) {
        this.login = login;
        this.renew = renew;
        this.sleep = sleep;
        this.clock = clock;
        this.random = random;
        this.tokenInitialized = new CountDownLatch(1);
        this.tokenRef = new AtomicReference<>(null);
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

                            // tokens renewal periods may change as they are renewed, as a result of MaxTTLs,
                            // differing initial TTL vs renewal period configurations or changes of configuration
                            // to the auth backend on the server.
                            //
                            // Because of that, we recalculate our base "grace" period any time we get a new ttl from Vault
                            // but because our "expiration" times on tokens are fuzzy guesses, we only treat an expiration
                            // as "new" if it is at least a minute older than the previous expiration high watermark
                            if (true || renewed.expiration.isAfter(expiration.plus(Duration.ofMinutes(1)))) {
                                gracePeriod = calculateGrace(Duration.between(now, expiration));
                            }
                            expiration = renewed.expiration;
                            this.tokenRef.set(renewed);
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
        ExponentialBackoff backoff = new ExponentialBackoff(this, sleep, Duration.ofSeconds(1), Duration.ofMinutes(8));
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

    private final class ExponentialBackoff {

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

    // now is passed as an argument to capture time before the renewal call because we pessimistically
    // assume any latency on the network call happened while the token is being sent back and should be counted againstthe TTL;
    private TokenWithExpiration addExpiration(Instant now, AuthResponse token) {
        final Duration ttl = Duration.ofSeconds(token.getAuthLeaseDuration());
        final Instant expiration = now.plus(ttl);
        return new TokenWithExpiration(token, expiration);
    }

    public static VaultTokenLifecycle create(LifecycleConfig config) {
        return new VaultTokenLifecycle(config.login, config.renew, Clock.systemUTC(), new WallClockSleep(), new Random());
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
