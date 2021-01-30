package io.ianferguson.vault.api;

import io.ianferguson.vault.Vault;
import io.ianferguson.vault.VaultException;
import io.ianferguson.vault.response.AuthResponse;
import io.ianferguson.vault.util.VaultContainer;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Integration tests for the Username/Password auth backend.
 */
public class AuthBackendUserPassTests {

    @ClassRule
    public static final VaultContainer container = new VaultContainer();

    @BeforeClass
    public static void setupClass() throws IOException, InterruptedException {
        container.initAndUnsealVault();
        container.setupBackendUserPass();
    }

    /**
     * Test Authentication with new userpass auth backend
     */
    @Test
    public void testLoginByUserPass() throws VaultException {
        final Vault vault = container.getVault();
        final AuthResponse response = vault.auth().loginByUserPass(VaultContainer.USER_ID, VaultContainer.PASSWORD);
        final String token = response.getAuthClientToken();

        assertNotNull(token);
        assertNotSame("", token.trim());
    }

}
