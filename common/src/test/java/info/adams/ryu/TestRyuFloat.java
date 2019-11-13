package info.adams.ryu;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRyuFloat {
    @Test
    public void testFloatToBuffer() {
        float f = 0.33007812f;
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();

        int size = RyuFloat.floatToString(buffer, f, RoundingMode.ROUND_EVEN);

        assertThat(size).isEqualTo(10);
        assertThat(ByteBufUtil.hexDump(buffer)).isEqualTo("302e3333303037383132");
        assertThat(buffer.toString(UTF_8)).isEqualTo("0.33007812");
    }
}
