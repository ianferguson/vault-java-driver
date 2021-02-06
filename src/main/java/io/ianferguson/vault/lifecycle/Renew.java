package io.ianferguson.vault.lifecycle;

import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;

interface Renew {

    AuthResponse renew(AuthResponse token) throws VaultException;

}