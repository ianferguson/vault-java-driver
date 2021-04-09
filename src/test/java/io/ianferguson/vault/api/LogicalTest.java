package io.ianferguson.vault.api;

import io.ianferguson.vault.VaultConfig;
import io.ianferguson.vault.json.JsonObject;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LogicalTest extends TestCase {

    public void testMapToJsonObject() {
        Logical logical = new Logical(new VaultConfig());
        Map<String, Object> data = new HashMap<>();
        ArrayList testList = new ArrayList<>();
        testList.add("value1");
        testList.add(2);
        testList.add(true);
        data.put("ArrayValue", testList);
        data.put("IntValue", 1);
        data.put("BoolValue", false);
        JsonObject jsonObject = logical.mapToJsonObject(data);

        String expectedValue = "{\"BoolValue\":false,\"ArrayValue\":[\"value1\",2,true],\"IntValue\":1}";
        assertTrue(jsonObject.toString().equals(expectedValue));
    }
}