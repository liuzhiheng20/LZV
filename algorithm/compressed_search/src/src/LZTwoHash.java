import net.jpountz.util.UnsafeUtils;
import net.jpountz.util.Utils;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

public class LZTwoHash {

    private static final long A = 2654435761L; // 32-bit odd integer (golden ratio prime)
    private static final int M = 32;                // Input bit-length
    private static final int N = 17;

    private static final int HASH_TABLE_SIZE = 1 << N;
    private static final int HASH_TABLE_SIZE_BIG = 1 << (N);
    private static final int WINDOW_SIZE = 256 * 256;
    private static final int MIN_MATCH = 4;
    private static int val1 = 0; // 用于存储匹配的4字节整数值

    static XXHashFactory factory = XXHashFactory.fastestInstance();
    static XXHash32 hash32 = factory.hash32();
    static int SEED = 0; // Hash seed
    static StreamingXXHash streamingXXHash = new StreamingXXHash(SEED);
    static int hashTemp = 0;
    static int matchLenTemp = 0;
    static int candidateTemp = 0;
    //static LongHashFunction xxHash32 = LongHashFunction.xx();

    public static byte[] compress(byte[] src) {
        ByteBuffer out = ByteBuffer.allocate(src.length + src.length / 255 + 16);
        int[] hashTableNear = new int[HASH_TABLE_SIZE];  // 原始的Hash表
        int[] hashTableFar = new int[HASH_TABLE_SIZE_BIG];  // 用于记录相对较远的位置（WINDOW_SIZE/2）
        Arrays.fill(hashTableNear, -1);
        Arrays.fill(hashTableFar, -1);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length-12) {
            // 查找最长匹配
            int matchPos = findHashPlusMatch(src, hashTableNear, hashTableFar, srcPtr);
            if (matchPos != -1 && srcPtr - matchPos <= 0xFFFF && srcPtr - anchor <= 0xFFFFF) {
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                int offset = srcPtr - matchPos;
                // 写入Literal长度（先写高4位）
                writeLiteralLength(out, srcPtr - anchor, matchLen);
                // 拷贝Literal数据
                out.put(src, anchor, srcPtr - anchor);
                // 写入Match信息
                writeMatch(out, offset, matchLen);
                // 更新Hash表
                updateHashTablePlus(src, hashTableFar, srcPtr, matchLen);
                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }
        srcPtr = src.length;

        // 处理最后的Literal
        writeLiteralLength(out, srcPtr - anchor, 4);
        out.put(src, anchor, srcPtr - anchor);
        return Arrays.copyOf(out.array(), out.position());
    }

    private static int findHashPlusMatch(byte[] src, int[] hashTableShort, int[] hashTableLong, int pos) {
        // 使用改进后的hash表快速查找匹配,返回的是最佳匹配的起始位置，使用两个hash
        if (pos + MIN_MATCH > src.length) return -1;
        hashTemp = hash(src, pos);
        candidateTemp = hashTableShort[hashTemp];
        if (candidateTemp ==-1)
            candidateTemp = hashTableLong[hashTemp];
        // 判断candidate位置的匹配长度
        matchLenTemp = 0;
        if(candidateTemp!=-1)
            matchLenTemp = findMatchLength(src, candidateTemp, pos);
        if(candidateTemp ==-1 || pos - candidateTemp > WINDOW_SIZE/2 || matchLenTemp < 4) {
            hashTableShort[hashTemp] = pos;  // 说明以前的hash的位置记录的并不是
        }
        if(matchLenTemp >= 4){
            // 查看长度加一的hash值是否在表中，查看那个位置的匹配长度是否真的更长
            // 这里应该增加递归查找
            while(true) {
                int hash2 = streamingHash(src, pos, matchLenTemp+1);
                int candidate2 = hashTableLong[hash2];
                int matchLen2 = 0;
                if (candidate2 != -1) {
                    matchLen2 = findMatchLength(src, candidate2, pos);
                }
                if(candidate2 == -1 || pos - candidate2 > WINDOW_SIZE/2 || matchLen2 < 4) {
                    hashTableLong[hash2] = pos;
                }
                if(candidate2 == pos) {
                    break;
                }
                if(matchLen2 > matchLenTemp) {
                    candidateTemp = candidate2;
                    matchLenTemp = matchLen2;
                } else {
                    break;
                }
            }
        }
        if(candidateTemp!=-1) {
            for(int i=0;i<4;i++) {
                if(src[candidateTemp+i]!=src[pos+i]) {
                    return -1;
                }
            }
        }

        return (candidateTemp != -1 && pos - candidateTemp <= 0xFFFF) ? candidateTemp : -1;
    }

    private static void updateHashTablePlus(byte[] src, int[] tableLong, int srcPtr, int matchLen) {
        int end = srcPtr + matchLen;
        streamingXXHash.processFirstChunk(src, end-3);
        for (int i = end - 4; i >= srcPtr; i--) {// Reverse iteration for long-distance match updates
            hashTemp = streamingHash(src[i]);  // Long-distance match
            //int hash = streamingHash(src, i, end-i+1);
            //if (tableLong[hash] == -1 || i - tableLong[hash] > WINDOW_SIZE / 2) {  // todo:优化掉判断
                tableLong[hashTemp] = i;
            //}
        }
        // 对于xxxy,xxyy,xyyy这些情况要加入第二个hash表
        for(int i=end-3; i<end; i++) {
            if (i + 3 >= src.length) {
                break; // 剩下的不足3个字节，无法构成哈希
            }
            hashTemp = hash(src, i);
            if(tableLong[hashTemp]==-1 || i - tableLong[hashTemp] > WINDOW_SIZE/2) {
                tableLong[hashTemp] = i;
            }
        }
    }

    private static int hash(byte[] data, int pos) {
        int val = (data[pos] & 0xFF)
                | ((data[pos + 1] & 0xFF) << 8)
                | ((data[pos + 2] & 0xFF) << 16)
                | ((data[pos + 3] & 0xFF) << 24);
        int hash = hash(val);
        return hash;
    }

    static int hash(int i) {
//        return i * -1640531535 >>> 20;
        long product =(A) * (long)(i+13);
        product &= 0xFFFFFFFFL;
        return (int) (product >>> (M - N));
    }

//    private static int hash(byte[] data, int pos, int len) {
//        // 对从pos开始的len个字节进行hash
//        int hash = 0;
//        if(pos+len<data.length) {
//            hash = ((hash32.hash(data, pos, len, SEED) % HASH_TABLE_SIZE_BIG)+HASH_TABLE_SIZE_BIG)%HASH_TABLE_SIZE_BIG;
//        }
//        return hash;
//    }

//    private static int streamingHash(byte[] data, int pos, int len) {
//        // 对从pos开始的len个字节进行hash
//        int hash = 0;
//        if(pos+len < data.length) {
//            streamingXXHash.update(data, pos, len);
//            hash = streamingXXHash.getValue();
//            hash = ((hash % HASH_TABLE_SIZE_BIG)+HASH_TABLE_SIZE_BIG)%HASH_TABLE_SIZE_BIG;
//        }
//        return hash;
//    }
    private static int streamingHash(byte[] data, int pos, int len) {
        //long hash = 0;
        if(pos+len < data.length) {
            streamingXXHash.update(data, pos, len);
            return streamingXXHash.getValue()% HASH_TABLE_SIZE_BIG;
            //hash = ((hash % HASH_TABLE_SIZE_BIG)+HASH_TABLE_SIZE_BIG)%HASH_TABLE_SIZE_BIG;
        }
        return 0;
    }

    private static int streamingHash(byte b) {
        // 对从pos开始的len个字节进行hash
        streamingXXHash.update(b);
        return streamingXXHash.getValue()% HASH_TABLE_SIZE_BIG;
        //hash = ((hash % HASH_TABLE_SIZE_BIG)+HASH_TABLE_SIZE_BIG)%HASH_TABLE_SIZE_BIG;
        //return hash;
    }

//    private static int findMatchLength(byte[] src, int matchPos, int srcPos) {
//        int maxLen = Math.min(src.length - srcPos, 0xFFFF + MIN_MATCH);
//        int len = 0;
//        while (len < maxLen && src[matchPos + len] == src[srcPos + len]) {
//            len++;
//        }
//        //System.out.println("matchLen: " + len + ", maxLen: "+maxLen);
//        return len >= MIN_MATCH ? len : 0;
//    }

//    private static int findMatchLength(byte[] src, int matchPos, int srcPos) {
//        int maxLen = Math.min(src.length - srcPos - 8, 0xFFFF + MIN_MATCH);
//        int len = 0;
//
//        while (len + 4 <= maxLen) {
//            val1 = ByteBuffer.wrap(src, matchPos + len, 4).getInt();
////            int val2 =
//            if (val1 != ByteBuffer.wrap(src, srcPos + len, 4).getInt()) {
//                break;
//            }
//            len += 4;
//        }
//
//        while (len < maxLen && src[matchPos + len] == src[srcPos + len]) {
//            len++;
//        }
//
//        return len >= MIN_MATCH ? len : 0;
//    }

    static int findMatchLength(byte[] src, int ref, int sOff) {
        int srcLimit = src.length;
        int matchLen;
        for(matchLen = 0; sOff <= srcLimit - 8; sOff += 8) {
            if (UnsafeUtils.readLong(src, sOff) != UnsafeUtils.readLong(src, ref)) {
                int zeroBits;
                if (Utils.NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
                    zeroBits = Long.numberOfLeadingZeros(UnsafeUtils.readLong(src, sOff) ^ UnsafeUtils.readLong(src, ref));
                } else {
                    zeroBits = Long.numberOfTrailingZeros(UnsafeUtils.readLong(src, sOff) ^ UnsafeUtils.readLong(src, ref));
                }

                return matchLen + (zeroBits >>> 3);
            }

            matchLen += 8;
            ref += 8;
        }

        while(sOff < srcLimit && UnsafeUtils.readByte(src, ref++) == UnsafeUtils.readByte(src, sOff++)) {
            ++matchLen;
        }

        return matchLen;
    }

    private static void writeLiteralLength(ByteBuffer out, int literalLen, int matchLen) {
        matchLen -= MIN_MATCH;
        if (literalLen >= 15) {
            if(matchLen>=15){
                out.put((byte) 0xFF);
            } else{
                out.put((byte) (0xF0 | (matchLen & 0x0F)));
            }

            literalLen -= 15;
            while (literalLen >= 255) {
                out.put((byte) 0xFF);
                literalLen -= 255;
            }
            out.put((byte) literalLen);
        } else {
            byte temp = (byte) ((literalLen << 4) & 0xF0);
            if(matchLen>=15){
                temp |= 0x0F;
            } else{
                temp |= (matchLen & 0x0F);
            }
            out.put(temp);
        }
    }

    private static void writeMatch(ByteBuffer out, int offset, int matchLen) {
        // 小端序写入offset
        out.put((byte) offset);
        out.put((byte) (offset >>> 8));
        matchLen -= MIN_MATCH;
        if (matchLen >= 15) {
            matchLen -= 15;
            while (matchLen >= 255) {
                out.put((byte) 0xFF);
                matchLen -= 255;
            }
            out.put((byte) matchLen);
        }
    }

    public static boolean multipleCompressTest() {
        final int COMPRESS_NUM = 1000000; // 减少测试数量便于演示
        final int DATA_LEN = 10;
        Random rand = new Random();

        for (int count = 0; count < COMPRESS_NUM; count++) {
            System.out.println("Test iteration: " + count);

            int[] inputArray = new int[DATA_LEN];
            for (int i = 0; i < DATA_LEN; i++) {
                inputArray[i] = rand.nextInt(101);
            }
            System.out.println(Arrays.toString(inputArray));

            byte[] originalData = utils.intToBytes(inputArray);

            // 压缩
            byte[] compressed = compress(originalData);

            // 解压
            byte[] decompressed = LZ4.LZ4Decompress(compressed, originalData.length);
            if (!Arrays.equals(originalData, decompressed)) {
                System.out.println("Test failed!");
                System.out.println("Original data: " + Arrays.toString(originalData));
                return false;
            }
        }
        return true;
    }

    public static boolean singleCompressTest() {
        // 70, 41, 65, 4, 82, 11, 31, 42, 34, 59, 58, 35, 77, 80, 72, 79, 61, 5, 53, 12, 18, 65, 13, 42, 34, 59, 58, 87, 81, 37
        int[] inputArray = new int[]{29, 79, 24, 52, 25, 30, 65, 69, 0, 63};
        //int[] inputArray = new int[]{41, 65, 11, 31, 42, 34, 59, 58, 35, 18, 65, 13, 42, 34, 59, 58, 87, 81, 37};
        System.out.println(Arrays.toString(inputArray));

        byte[] originalData = utils.intToBytes(inputArray);

        // 压缩
        byte[] compressed = compress(originalData);
        byte[] decompressed2 = LZ4.LZ4Decompress(compressed, originalData.length);
        System.out.println("compressed data:" + Arrays.toString(compressed));
        System.out.println("LZ4 compressed data:" + Arrays.toString(decompressed2));
        for (byte b : decompressed2) {
            System.out.println(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0') + " ");
        }

        // 解压
        //byte[] decompressed = decompress(compressed);
        if (!Arrays.equals(originalData, decompressed2)) {
            System.out.println("Test failed!");
            System.out.println("Original data: " + Arrays.toString(originalData));
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        // 单次压缩解压测试
        // multipleCompressTest();
        singleCompressTest();
    }
}
