package info.adams.ryu;

import org.apache.commons.codec.binary.Hex;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRyuFloat {
    @Test
    public void testFloatToBuffer() {
        float f = 0.33007812f;
        final ByteBuffer buffer = ByteBuffer.allocate(10);

        int size = RyuFloat.floatToString(buffer, f, RoundingMode.ROUND_EVEN);

        assertThat(size).isEqualTo(10);
        assertThat(Hex.encodeHexString(buffer.array())).isEqualTo("302e3333303037383132");
        assertThat(new String(buffer.array(), UTF_8)).isEqualTo("0.33007812");
    }
}
