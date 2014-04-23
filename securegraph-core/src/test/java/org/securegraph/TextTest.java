package org.securegraph;

import org.securegraph.util.JavaSerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TextTest {
    @Test
    public void testSerializations() {
        byte[] bytes = JavaSerializableUtils.objectToBytes(new Text("test", TextIndexHint.ALL));

        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        System.out.println(sb.toString());
        sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%c  ", (char) b));
        }
        System.out.println(sb.toString());

        assertTrue("serialization resulted in large data (bytes: " + bytes.length + ")", bytes.length < 110);

        Text t = (Text) JavaSerializableUtils.bytesToObject(bytes);
        assertEquals("test", t.getText());
        assertEquals(TextIndexHint.ALL, t.getIndexHint());
    }
}
