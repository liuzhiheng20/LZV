import java.util.Random;

public class StreamingXXHash {  // 修改为：以4个字节为一组，拆成4个1字节的byte

    // xxHash32 使用的 5 个质数常量，算法内部用来混合数据
    private static final int PRIME1 = 0x9E3779B1;
    private static final int PRIME2 = 0x85EBCA77;
    private static final int PRIME3 = 0xC2B2AE3D;
    private static final int PRIME4 = 0x27D4EB2F;
    private static final int PRIME5 = 0x165667B1;

    private int seed;       // 用户提供的种子值
    private int totalLen = 0;     // 总共处理过的字节数

    // 四个累加器 v1~v4（xxHash 会使用四路并行计算优化性能）
    private int v1, v2, v3, v4;

    // 缓冲区：存储不足 16 字节的“零散”数据
    private final byte[] buffer = new byte[4];
    private int bufferSize = 0;

    // 构造函数，指定 seed 值
    public StreamingXXHash(int seed) {
        this.seed = seed;
        resetState();
    }

    // 初始化四个累加器状态
    public void resetState() {
        v1 = seed + PRIME1 + PRIME2;
        v2 = seed + PRIME2;
        v3 = seed;
        v4 = seed - PRIME1;
        totalLen = 0;
        bufferSize = 0;
    }

    // 每次追加一个字节
    public void update(byte b) {
        switch (totalLen & 3) {
            case 0: v1 = round(v1, b); break;
            case 1: v2 = round(v2, b); break;
            case 2: v3 = round(v3, b); break;
            case 3: v4 = round(v4, b); break;
        }
//        if(totalLen % 4 == 0) {
//            v1 = round(v1, b);
//        } else if (totalLen % 4 == 1) {
//            v2 = round(v2, b);
//        } else if (totalLen % 4 == 2) {
//            v3 = round(v3, b);
//        } else {
//            v4 = round(v4, b);
//        }
        totalLen++;
    }

    // 支持多字节批量更新（内部调用单字节 update, 从后往前更新，这是因为我们的hash计算的关系）
    public void update(byte[] data, int off, int len) {
        resetState();   // 无论是查询时的多序列还是匹配时的多序列出现在第一步或者唯一一部都需要更新原始状态
        for (int i = len-1; i >= 0; i--) {
            update(data[off + i]);
        }
    }

    // 获取当前的 hash 值（含未满16字节尾部处理 + 雪崩混合）
    public int getValue() {
        int h32;
        // 处理过完整 4 字节块
        if (totalLen >= 4) {
            h32 = Integer.rotateLeft(v1, 1)
                    + Integer.rotateLeft(v2, 7)
                    + Integer.rotateLeft(v3, 12)
                    + Integer.rotateLeft(v4, 18);
        } else {
            // 只处理了小数据，使用 seed + PRIME5 起始值
            h32 = seed + PRIME5;
        }

        h32 += totalLen;  // 混入总长度

        // 雪崩混合，扩散位
        h32 ^= h32 >>> 15;
        h32 *= PRIME2;
        h32 ^= h32 >>> 13;
        h32 *= PRIME3;
        h32 ^= h32 >>> 16;

        return h32& 0x7FFFFFFF;
    }

    // first chunk的长度一定是4个byte
    public void processFirstChunk(byte[] buf, int offset) {
        resetState();
        if(offset + 3 < buf.length) {
            v1 = round(v1, buf[offset + 3]);
            v2 = round(v2, buf[offset + 2]);
            v3 = round(v3, buf[offset + 1]);
            v4 = round(v4, buf[offset]);
            totalLen = 4;
        }
    }

    // 单次混合函数：核心变换逻辑（每个 v 值与一个 int 输入混合）
    private static int round(int acc, int input) {
        acc += input * PRIME2;
        acc = Integer.rotateLeft(acc, 13);
        acc *= PRIME1;
        return acc;
    }

    // 按小端字节序读取 4 字节 int（Java 默认是大端，这里模拟 xxHash 的小端序）
    private static int getByteLE(byte[] buf, int index) {
        return (buf[index] & 0xFF);
    }

    public static void main(String[] args) {
        int n = 10000; // Number of repetitions
        int m = 100; // Length of the byte array
        Random random = new Random();

        for (int i = 0; i < n; i++) {
            // Generate a random byte array of length m
            byte[] data = new byte[m];
            random.nextBytes(data);

            // Hash value by updating one byte at a time
            StreamingXXHash hashOneByteAtATime = new StreamingXXHash(0);
            for (int j = data.length - 1; j >= 0; j--) {
                hashOneByteAtATime.update(data[j]);
            }
            int valueOneByteAtATime = hashOneByteAtATime.getValue();

            // Hash value by updating the entire array at once
            StreamingXXHash hashWholeArray = new StreamingXXHash(0);
            hashWholeArray.update(data, 0, data.length);
            int valueWholeArray = hashWholeArray.getValue();

            // Compare the results
            if(valueWholeArray!=valueOneByteAtATime) {
                System.out.println(valueOneByteAtATime + " " + valueWholeArray);
            }
        }
    }
}
