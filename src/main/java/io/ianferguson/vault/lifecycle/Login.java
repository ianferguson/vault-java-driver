package io.ianferguson.vault.lifecycle;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;

interface Login {

    AuthResponse login() throws VaultException;

}