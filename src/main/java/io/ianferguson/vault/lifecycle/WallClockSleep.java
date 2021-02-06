package io.ianferguson.vault.lifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class WallClockSleep implements Sleep {

    @Override
    public void sleep(Duration duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // TKTK log interrupt, check Guava's similar sleepers behavior in this
            Thread.currentThread().interrupt();
        }
    }

}