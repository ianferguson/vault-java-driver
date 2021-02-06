package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
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

public class LifecycleTest {

    private static final TestClock CLOCK = new TestClock();

    @Test
    public void test() throws VaultException, InterruptedException, ExecutionException {

        final Sleep sleep = spinSleep(CLOCK);

        final Logins logins = Logins.create();

        final Login login = logins::login;
        final Renew renew = token -> {
            return logins.renew(token.getAuthClientToken());
        };

        final AuthResponse token = login.login();
        final EternalLifecycle lifecycle = new EternalLifecycle(login, renew, token, CLOCK, sleep);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<?> future = executor.submit(lifecycle);

        final Instant end = CLOCK.instant().plus(Duration.ofDays(8));
        final Random random = new Random();
        while (CLOCK.instant().isBefore(end)) {
            //  Thread.sleep(0, 1);
            // advance the clock on average, but not exactly, a millisecond each tick
            CLOCK.advance(Duration.ofNanos(random.nextInt(2000000)));
        }

        // throw off any exception that came up in the runnable
        future.cancel(true);
        try {
            future.get();
        } catch (CancellationException e) {
            // expected
        }


        final long tokenCount = logins.tokens.size();
        long renewalCount = 0;
        for (Token t : logins.tokens.values()) {
            renewalCount += t.numberOfRenewals;
            System.out.println(t);
        }
        final double renewalsPerToken = (double) renewalCount / (double) tokenCount;
        System.out.println("Created " + tokenCount + " tokens, and averaged " + renewalsPerToken + " renewals per token");
    }

    private static final class Logins {

        // TKTK make timing tunable for tests
        // TKTK make MaxTTL an option
        private static final Duration oneHour = Duration.ofHours(1);
        private static final Duration fiveMinutes = Duration.ofMinutes(5);
        private static final double failureRate = 0.2;

        private final Random random = new Random();
        private final ConcurrentMap<String, Token> tokens = new ConcurrentHashMap<>();
        private final Clock clock;

        Logins(Clock clock) {
            this.clock = clock;
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

            final Token renewedToken = token.renewFor(oneHour);
            tokens.put(id, renewedToken);
            return renewedToken.asAuthResponse();
        }

        public static Logins create() {
            return new Logins(CLOCK);
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

}
