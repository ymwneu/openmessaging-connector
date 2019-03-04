package io.openmessaging.connect.runtime.common;

import io.openmessaging.KeyValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectKeyValueTest {

    @Test
    public void testKeyValueOperations(){

        KeyValue keyValue = new ConnectKeyValue();
        keyValue.put("StringKey", "StringValue");
        keyValue.put("IntegerKey", 0);
        keyValue.put("LongKey", 1L);
        keyValue.put("DoubleKey", 5.2);

        assertEquals("StringValue", keyValue.getString("StringKey"));
        assertEquals(0, keyValue.getInt("IntegerKey"));
        assertEquals(1L, keyValue.getLong("LongKey"));
        assertEquals(5.2, keyValue.getDouble("DoubleKey"), 0.0);

        assertEquals("StringValue1", keyValue.getString("StringKey1", "StringValue1"));
        assertEquals(2, keyValue.getInt("IntegerKey1", 2));
        assertEquals(2L, keyValue.getLong("LongKey1", 2L));
        assertEquals(5.0, keyValue.getDouble("DoubleKey1", 5.0), 0.0);

        Set<String> keySet = keyValue.keySet();
        Set<String> compareKeySet = new HashSet<>();
        compareKeySet.add("StringKey");
        compareKeySet.add("IntegerKey");
        compareKeySet.add("LongKey");
        compareKeySet.add("DoubleKey");

        assertEquals(keySet, compareKeySet);

        assertTrue(keyValue.containsKey("StringKey"));
        assertTrue(keyValue.containsKey("IntegerKey"));
        assertTrue(keyValue.containsKey("LongKey"));
        assertTrue(keyValue.containsKey("DoubleKey"));

    }
}
