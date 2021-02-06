package io.ianferguson.vault.lifecycle;

import static java.util.Objects.requireNonNull;

public final class LifecycleConfig {
    final Login login;
    final Renew renew;

    LifecycleConfig(Login login, Renew renew) {
        this.login = login;
        this.renew = renew;
    }

    public static LifecycleConfig.Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Login login;
        private Renew renew;

        public LifecycleConfig.Builder usingRenew(Renew renew) {
            this.renew = renew;
            return this;
        }

        public LifecycleConfig.Builder usingLogin(Login login) {
            this.login = login;
            return this;
        }

        public LifecycleConfig build() {
            final Login login = requireNonNull(this.login, "Must configure Login function for Lifecycle");
            final Renew renew = requireNonNull(this.renew, "Must configure Renew function for Lifecycle");
            return new LifecycleConfig(login, renew);
        }
    }
}