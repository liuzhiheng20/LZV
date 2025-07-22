import java.nio.*;

public class utils {
    // 提供一些基本就的工具方法
    public static byte[] intToBytes(int[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * 4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (int num : data) {
            buffer.putInt(num);
        }
        return buffer.array();
    }

    public static byte[] longToBytes(long[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * 8);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (long num : data) {
            buffer.putLong(num);
        }
        return buffer.array();
    }

    public static byte[] doubleToBytes(double[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * 8);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (double num : data) {
            buffer.putDouble(num);
        }
        return buffer.array();
    }

    public static byte[] numberToBytes(Number[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * 8);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (Number num : data) {
            if (num instanceof Integer) {
                buffer.putInt((Integer) num);
            } else if (num instanceof Long) {
                buffer.putLong((Long) num);
            } else if (num instanceof Double) {
                buffer.putDouble((Double) num);
            } else {
                throw new IllegalArgumentException("Unsupported number type");
            }
        }
        byte[] usedArray = new byte[buffer.position()];
        buffer.flip();
        buffer.get(usedArray);
        return usedArray;
    }

    public static int[] bytesToInts(byte[] byteData) {
        if (byteData.length % 4 != 0) {
            throw new IllegalArgumentException("Byte data length must be multiple of 4");
        }
        IntBuffer intBuffer = ByteBuffer.wrap(byteData)
                .order(ByteOrder.BIG_ENDIAN)
                .asIntBuffer();
        int[] result = new int[intBuffer.remaining()];
        intBuffer.get(result);
        return result;
    }

    public static int bytesToInt(byte[] byteData) {
        if (byteData.length != 4) {
            throw new IllegalArgumentException("Byte data length must be exactly 4");
        }
        return ByteBuffer.wrap(byteData)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    public static long[] bytesToLongs(byte[] byteData) {
        if (byteData.length % 8 != 0) {
            throw new IllegalArgumentException("Byte data length must be multiple of 8");
        }
        ByteBuffer buffer = ByteBuffer.wrap(byteData).order(ByteOrder.BIG_ENDIAN);
        LongBuffer longBuffer = buffer.asLongBuffer();
        long[] result = new long[longBuffer.remaining()];
        longBuffer.get(result);
        return result;
    }

    public static long bytesToLong(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException("Byte array must be exactly 8 bytes long");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.BIG_ENDIAN); // 可改为 LITTLE_ENDIAN，如果需要小端序
        return buffer.getLong();
    }

    public static double[] bytesToDouble(byte[] byteData) {
        if (byteData.length % 8 != 0) {
            throw new IllegalArgumentException("Byte data length must be multiple of 8");
        }
        ByteBuffer buffer = ByteBuffer.wrap(byteData).order(ByteOrder.BIG_ENDIAN);
        DoubleBuffer doubleBuffer = buffer.asDoubleBuffer();
        double[] result = new double[doubleBuffer.remaining()];
        doubleBuffer.get(result);
        return result;
    }
}
