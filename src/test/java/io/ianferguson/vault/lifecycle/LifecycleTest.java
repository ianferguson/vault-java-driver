package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;
import io.ianferguson.vault.rest.RestResponse;

import static org.junit.Assert.assertTrue;

public class LifecycleTest {

    private static final TestClock CLOCK = new TestClock();

    @Test
    public void test() throws VaultException, InterruptedException, ExecutionException {
        // the random number generators used throughout the tests are kept stable, but the actual
        // tests are still not stable -- the clock action isn't centrally gated and runs as
        // fast as the computer will run that particular thread
        final long seed = 2948468929534380l;
        final Randoms randoms = new Randoms(seed);

        final Sleep sleep = spinSleep(CLOCK);
        final Logins logins = Logins.create(randoms.get());
        final Login login = logins::login;
        final Renew renew = token -> {
            return logins.renew(token.getAuthClientToken());
        };
        final EternalLifecycle lifecycle = new EternalLifecycle(login, renew, null, CLOCK, sleep, randoms.get());

        final ExecutorService executor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        final Future<?> lifecycleFuture = executor.submit(lifecycle);

        final Random clockRandomizer = randoms.get();
        executor.submit(() -> {
            while (true) {
                // advance the clock on average, but not exactly, 1 minute each tick
                Thread.sleep(1);
                CLOCK.advance(Duration.ofSeconds(clockRandomizer.nextInt(120)));
            }
        });

        // continually read the current token and validate if it is still valid
        final Instant end = CLOCK.instant().plus(Duration.ofDays(8));
        long validToken = 0;
        long invalidToken = 0;
        while (CLOCK.instant().isBefore(end)) {
            final AuthResponse token = lifecycle.getToken();
            if (token == null) {
                invalidToken++;
                continue;
            }

            final String id = token.getAuthClientToken();
            if (logins.isValid(id)) {
                validToken++;
            } else {
                invalidToken++;
            }
        }

        // print statistics
        final long tokenCount = logins.tokens.size();
        long renewalCount = 0;
        for (Token t : logins.tokens.values()) {
            renewalCount += t.numberOfRenewals;
            System.out.println(t);
        }
        final String renewalsPerToken = String.format("%,.2f", (double) renewalCount / (double) tokenCount);
        System.out.println("Created " + tokenCount + " tokens, and averaged " + renewalsPerToken + " renewals per token");
        final long totalCalls = validToken + invalidToken;
        final double percent = validToken / (double) totalCalls;
        final String percentString = String.format("%,.2f", percent * 100);
        System.out.println(percentString + "% of client token uses were valid (" + validToken + "/" + totalCalls + ")");

        assertTrue(percent > 0.999);

        // check for errors
        if (lifecycleFuture.isDone()) {
            lifecycleFuture.get();
        }
    }

    private static final class Logins {
        // TKTK make timing tunable for tests
        // TKTK make MaxTTL an option
        private static final Duration oneHour = Duration.ofHours(1);
        private static final Duration twentyMinutes = Duration.ofMinutes(20);
        private static final double failureRate = 0.2;

        private final ConcurrentMap<String, Token> tokens = new ConcurrentHashMap<>();
        private final Clock clock;
        private final Random random;

        Logins(Random random, Clock clock) {
            this.clock = clock;
            this.random = random;
        }

        AuthResponse login() throws VaultException {
            simulateInstability();

            final Instant expiration = clock.instant().plus(oneHour);
            final Token token = Token.expiringAt(expiration);
            tokens.put(token.id, token);
            return token.asAuthResponse();
        }

        AuthResponse renew(String id) throws VaultException {
            simulateInstability();

            final Token token = tokens.get(id);
            if (token == null) {
                final String msg = "[FAILURE] token " + id + " does not exist and cannot be renewed";
                throw new VaultException(msg, 403);
            }

            final Instant now = this.clock.instant();
            if (this.clock.instant().isAfter(token.expiresAt)) {
                final String msg = "[FAILURE] token " + token + " cannot be renewed.\nit expired at (" + token.expiresAt + ") but it is currently (" + now + ")";
                throw new VaultException(msg, 403);
            }

            final Token renewedToken = token.renewFor(twentyMinutes);
            tokens.put(id, renewedToken);
            return renewedToken.asAuthResponse();
        }

        boolean isValid(String id) {
            return this.tokens.get(id).expiresAt.isAfter(CLOCK.instant());
        }

        public static Logins create(Random random) {
            return new Logins(random, CLOCK);
        }

        private void simulateInstability() throws VaultException {
            if (this.random.nextDouble() < failureRate) {
                throw new VaultException("simulated vault availability exception", 500);
            }
        }

    }

    private static final class Token {
        private final String id;
        private final long numberOfRenewals;
        private final Instant expiresAt;

        Token(String id, Instant expiresAt, long numberOfRenewals) {
            this.id = id;
            this.expiresAt = expiresAt;
            this.numberOfRenewals = numberOfRenewals;
        }

        @Override
        public String toString() {
            return "Token[id=" + this.id + ", renew count=" + this.numberOfRenewals  + ", expires at=" + this.expiresAt.truncatedTo(ChronoUnit.SECONDS) + "]";
        }

        Token renewFor(Duration duration) {
            return new Token(this.id, this.expiresAt.plus(duration), this.numberOfRenewals + 1);
        }

        AuthResponse asAuthResponse() {
            final Duration ttl = Duration.between(CLOCK.instant(), this.expiresAt);
            final String body = "{\n"
                    + "  \"request_id\": \"b1c6cd21-64cc-3ee8-3be2-02438e91a863\",\n"
                    + "  \"lease_id\": \"\",\n"
                    + "  \"lease_duration\": " + ttl.getSeconds() + ",\n"
                    + "  \"renewable\": false,\n"
                    + "  \"data\": null,\n"
                    + "  \"warnings\": null,\n"
                    + "  \"auth\": {\n"
                    + "    \"client_token\": \"" + id + "\",\n"
                    + "    \"accessor\": \"MBO09EWIAPxGnHCQdZ1EB88N\",\n"
                    + "    \"policies\": [\n"
                    + "      \"root\"\n"
                    + "    ],\n"
                    + "    \"token_policies\": [\n"
                    + "      \"policyname\"\n"
                    + "    ],\n"
                    + "    \"identity_policies\": null,\n"
                    + "    \"metadata\": null,\n"
                    + "    \"orphan\": false,\n"
                    + "    \"entity_id\": \"\",\n"
                    + "    \"lease_duration\": " + ttl.getSeconds() + ",\n"
                    + "    \"renewable\": true\n"
                    + "  }\n"
                    + "}";

            return new AuthResponse(new RestResponse(200, "application/json", body.getBytes()), 0);
        }

        static Token expiringAt(Instant expiresAt) {
            return new Token(UUID.randomUUID().toString(), expiresAt, 0);
        }
    }

    private static Sleep spinSleep(Clock clock) {
        return duration -> {
            final Instant now = clock.instant();
            final Instant stop = now.plus(duration);
            while (clock.instant().isBefore(stop)) {
                continue;
            }
        };
    }

    private static final class TestClock extends Clock {

        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

        TestClock() {
            println("Test clock started");
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration duration) {
            final Instant current = now.get();
            final Instant next = current.plus(duration);
            // if multiple test threads were writing to the clock at once, we would use a CAS here
            now.set(next);
        }

        void println(String line) {
            System.out.println("[" + instant().truncatedTo(ChronoUnit.SECONDS) + "] " + line);
        }

    }

    private static final class Randoms {

        private Random random;

        Randoms(long seed) {
            this.random = new Random(seed);
        }

        public Random get() {
            return new Random(this.random.nextLong());
        }

    }

}
