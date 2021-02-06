package io.ianferguson.vault.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;
import io.ianferguson.vault.rest.RestResponse;

public class LifecycleTest {

    private static final TestClock CLOCK = new TestClock();

    @Test
    public void test() throws VaultException, InterruptedException {

        final Sleep sleep = spinSleep(CLOCK);

        final Duration oneHour = Duration.ofHours(1);
        final Duration fiveMinutes = Duration.ofMinutes(5);
        final Login login = loginWithTTL(CLOCK, oneHour);
        final Renew renew = renewWithTTL(CLOCK, fiveMinutes);
        final AuthResponse token = login.login();

        final EternalLifecycle lifecycle = new EternalLifecycle(login, renew, token, CLOCK, sleep);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(lifecycle);

        final Instant end = CLOCK.instant().plus(Duration.ofDays(8));
        while (CLOCK.instant().isBefore(end)) {
            CLOCK.advance(Duration.ofSeconds(1));
        }
    }

    private static Login loginWithTTL(Clock clock, Duration ttl) {
        return () -> {
            final AuthResponse response = tokenWithTTL(ttl);
            CLOCK.println("Logging in for " + ttl.getSeconds());
            return response;
        };
    }

    private static Renew renewWithTTL(Clock clock, Duration ttl) {
        return token -> {
            final AuthResponse response = tokenWithTTL(ttl);
            CLOCK.println("Renewing for " + ttl.getSeconds());
            return response;
        };
    }

    private static Sleep spinSleep(Clock clock) {
        return duration -> {
            final Instant now = clock.instant();
            final Instant stop = now.plus(duration);
            CLOCK.println("Sleeping until " + stop);
            while (clock.instant().isBefore(stop)) {
                continue;
            }
            CLOCK.println("Waking up from spin sleep");
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

            // log out every time an hour passes
            final Instant currentUnit = current.truncatedTo(ChronoUnit.HOURS);
            final Instant nextUnit = next.truncatedTo(ChronoUnit.HOURS);
            if (nextUnit.isAfter(currentUnit)) {
                println("Tick, Wall clock: " + Instant.now());
            }
        }

        void println(String line) {
            System.out.println("[" + instant() + "] " + line);
        }

    }

    private static AuthResponse tokenWithTTL(Duration ttl) {
        final String body = "{\n"
                + "  \"request_id\": \"b1c6cd21-64cc-3ee8-3be2-02438e91a863\",\n"
                + "  \"lease_id\": \"\",\n"
                + "  \"lease_duration\": " + ttl.getSeconds() + ",\n"
                + "  \"renewable\": false,\n"
                + "  \"data\": null,\n"
                + "  \"warnings\": null,\n"
                + "  \"auth\": {\n"
                + "    \"client_token\": \"s.notreal\",\n"
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
}
