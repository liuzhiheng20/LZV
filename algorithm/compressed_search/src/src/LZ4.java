import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.Snappy;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;

public class LZ4 {

    private static final long A = 2654435761L; // 32-bit odd integer (golden ratio prime)
    private static final int M = 32;                // Input bit-length
    private static final int N = 17;                // Output bit-length

    // LZ4 Block格式常量
    private static final int BLOCK_SIZE = 64 * 1024 * 16;
    private static final int MIN_MATCH = 4;
    private static final int HASH_LOG = 20;
    private static final int HASH_TABLE_SIZE = 1 << N;
    private static final int WINDOW_SIZE = 256 * 256-2;

    static XXHashFactory factory = XXHashFactory.fastestInstance();
    static XXHash32 hash32 = factory.hash32();
    static int SEED = 0; // Hash seed

    static public int hashConflict = 0;//哈希冲突次数
    static public int hashTotal = 0;//哈希总次数


    public static byte[] LZ4Compress(byte[] src) {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        int maxCompressedLength = factory.fastCompressor().maxCompressedLength(src.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = factory.fastCompressor().compress(src, 0, src.length, compressed, 0, maxCompressedLength);
        return Arrays.copyOf(compressed, compressedLength);
    }

//    public static byte[] LZ4Compress(byte[] input) {
//        if (input == null || input.length == 0) return new byte[0];
//        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
//             FramedLZ4CompressorOutputStream lz4Out = new FramedLZ4CompressorOutputStream(baos)) {
//            lz4Out.write(input);
//            lz4Out.finish();
//            return baos.toByteArray();
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null; // 或抛出 RuntimeException
//        }
//    }

    public static byte[] LZ4CompressHigh(byte[] src) {
        LZ4Factory factory = LZ4Factory.safeInstance();
        int maxCompressedLength = factory.highCompressor().maxCompressedLength(src.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = factory.highCompressor().compress(src, 0, src.length, compressed, 0, maxCompressedLength);
        return Arrays.copyOf(compressed, compressedLength);
    }

//    public static byte[] gzipCompress(byte[] src) {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos)) {
//            gzipOut.write(src);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null; // 或者抛出异常，看你项目的需求
//        }
//        return bos.toByteArray();
//    }
    public static byte[] gzipCompress(byte[] src) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(bos)) {
            gzipOut.write(src);
            gzipOut.finish();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

//    public static byte[] snappyCompress(byte[] src) {
//        try {
//            return Snappy.compress(src);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null; // 你也可以选择抛出 RuntimeException
//        }
//    }
    public static byte[] snappyCompress(byte[] input) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             SnappyCompressorOutputStream snappyOut = new SnappyCompressorOutputStream(baos, input.length)) {
            snappyOut.write(input);
            snappyOut.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


//    public static byte[] lzmaCompress(byte[] src) {
//        if (src == null || src.length == 0) {
//            return new byte[0];
//        }
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        try (XZOutputStream xzOut = new XZOutputStream(bos, new LZMA2Options())) {
//            xzOut.write(src);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
//        return bos.toByteArray();
//    }
    public static byte[] lzmaCompress(byte[] src) {
        if (src == null || src.length == 0) {
            return new byte[0];
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             LZMACompressorOutputStream lzmaOut = new LZMACompressorOutputStream(bos)) {
            lzmaOut.write(src);
            lzmaOut.finish();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static byte[] LZ4Decompress(byte[] compressed, int originalLength) {
        try {
            LZ4Factory factory = LZ4Factory.fastestJavaInstance();
            byte[] restored = new byte[originalLength];
            factory.fastDecompressor().decompress(compressed, 0, restored, 0, originalLength);
            //factory.safeDecompressor().decompress(compressed, 0, compressed.length, restored, originalLength);
            return restored;
        }
        catch (LZ4Exception e) {
            System.err.println("LZ4 解压失败: " + e.getMessage());
            e.printStackTrace();
            return null; // 或者返回空数组 new byte[0]，或抛出 RuntimeException，看你的需求
        }
    }

    public static byte[] LZ4DecompressHigh(byte[] compressed, int originalLength) {
        try {
            LZ4Factory factory = LZ4Factory.safeInstance();
            byte[] restored = new byte[originalLength];
            factory.safeDecompressor().decompress(compressed, 0, compressed.length, restored, 0);
            return restored;
        }
        catch (LZ4Exception e) {
            System.err.println("LZ4 解压失败: " + e.getMessage());
            e.printStackTrace();
            return null; // 或者返回空数组 new byte[0]，或抛出 RuntimeException，看你的需求
        }
    }

    public static byte[] gzipDecompress(byte[] compressed) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIn.read(buffer)) > 0) {
                bos.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return bos.toByteArray();
    }

    public static byte[] snappyDecompress(byte[] compressed) {
        try {
            return Snappy.uncompress(compressed);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] lzmaDecompress(byte[] compressed) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (XZInputStream xzIn = new XZInputStream(new ByteArrayInputStream(compressed))) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = xzIn.read(buffer)) > 0) {
                bos.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return bos.toByteArray();
    }

    public static void clear() {
        hashConflict = 0;
        hashTotal = 0;
    }
    /**
     * LZ4标准压缩 (Block格式)
     *
     * @param src 输入数据
     * @return 兼容LZ4的解码数据
     */
    public static byte[] compress(byte[] src, boolean isHash, boolean isLazyHash) {
        ByteBuffer out = ByteBuffer.allocate(src.length + src.length / 255 + 16);
        int[] hashTable = new int[HASH_TABLE_SIZE];
        Arrays.fill(hashTable, -1);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length) {
            // 查找最长匹配
            int matchPos = -1;
            if(isHash) {
                matchPos = findHashMatch(src, hashTable, srcPtr, isLazyHash);
                //matchPos = findHashPlusMatch(src, hashTable, srcPtr);
            } else {
                matchPos = findBestMatch(src, srcPtr);
            }
            if (matchPos != -1 && srcPtr - matchPos <= 0xFFFF) {
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                //System.out.println("matchLen: " + matchLen);
                int offset = srcPtr - matchPos;
                // 写入Literal长度（先写高4位）
                writeLiteralLength(out, srcPtr - anchor, matchLen);
                // 拷贝Literal数据
                out.put(src, anchor, srcPtr - anchor);
                // 写入Match信息
                writeMatch(out, offset, matchLen);
                // 更新Hash表
                if(srcPtr - matchPos > WINDOW_SIZE/2 || !isLazyHash) {
                    updateHashTable(src, hashTable, srcPtr, matchLen);
                }
                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }

        // 处理最后的Literal
        writeLiteralLength(out, srcPtr - anchor, 0);
        out.put(src, anchor, srcPtr - anchor);
        return Arrays.copyOf(out.array(), out.position());
    }

    public static byte[] compress2Hash(byte[] src) {
        // 使用两个Hash表
        ByteBuffer out = ByteBuffer.allocate(src.length + src.length / 255 + 16);
        int[] hashTableNear = new int[HASH_TABLE_SIZE];  // 原始的Hash表
        int[] hashTableFar = new int[HASH_TABLE_SIZE];  // 用于记录相对较远的位置（WINDOW_SIZE/2）
        Arrays.fill(hashTableNear, -1);
        Arrays.fill(hashTableFar, -1);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length) {
            // 查找最长匹配
            //int matchPos = findHashMatch(src, hashTableNear, hashTableFar, srcPtr);
            int matchPos = findHashPlusMatch(src, hashTableNear, hashTableFar, srcPtr);
            if (matchPos != -1 && srcPtr - matchPos <= 0xFFFF && srcPtr - anchor <= 0xFFFFF) {
                //System.out.println("srcPtr: " + srcPtr + ", anchor: " + anchor + ", matchPos: " + matchPos);
                //int backwardMatchLen = findMatchLengthBackward(src, matchPos, srcPtr, anchor);
                //matchPos -= backwardMatchLen;
                //srcPtr -= backwardMatchLen;
                //System.out.println("backwardMatchLen: " + backwardMatchLen);
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                //System.out.println("matchLen: " + matchLen);
                int offset = srcPtr - matchPos;
                // 写入Literal长度（先写高4位）
                writeLiteralLength(out, srcPtr - anchor, matchLen);
                // 拷贝Literal数据
                out.put(src, anchor, srcPtr - anchor);
                // 写入Match信息
                writeMatch(out, offset, matchLen);
                // 更新Hash表
                //updateHashTable(src, hashTableNear, hashTableFar, srcPtr, matchLen);
                updateHashTablePlus(src, hashTableNear, hashTableFar, srcPtr, matchLen);
                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }

        // 处理最后的Literal
        writeLiteralLength(out, srcPtr - anchor, 0);
        out.put(src, anchor, srcPtr - anchor);
        return Arrays.copyOf(out.array(), out.position());
    }

    /**
     * LZ4标准压缩的索引部分
     *
     * @param src 输入数据
     * @return 只保留不同段之间的长度信息
     */
    public static byte[] compressIndex(byte[] src) {
        ByteBuffer out = ByteBuffer.allocate(src.length + src.length / 255 + 16);
        int[] hashTable = new int[HASH_TABLE_SIZE];
        //System.out.println("hash table size:"+HASH_TABLE_SIZE);
        Arrays.fill(hashTable, -1);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length) {
            // 查找最长匹配
            int matchPos = findHashMatch(src, hashTable, srcPtr, false);

            if (matchPos != -1 && srcPtr - matchPos <= 0xFFFF && srcPtr - anchor <= 0xFFFFF) {
                //System.out.println("srcPtr: " + srcPtr + ", anchor: " + anchor + ", matchPos: " + matchPos);
                int backwardMatchLen = findMatchLengthBackward(src, matchPos, srcPtr, anchor);
                matchPos -= backwardMatchLen;
                srcPtr -= backwardMatchLen;
                //System.out.println("backwardMatchLen: " + backwardMatchLen);
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                //System.out.println("matchLen: " + matchLen);
                int offset = srcPtr - matchPos;
                // 写入Literal长度（先写高4位）
                writeLiteralLength(out, srcPtr - anchor, matchLen);
                // 拷贝Literal数据
                // out.put(src, anchor, srcPtr - anchor);
                // 写入Match信息
                writeMatch(out, offset, matchLen);
                // 更新Hash表
                updateHashTable(src, hashTable, srcPtr, matchLen);

                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }

        // 处理最后的Literal
        writeLiteralLength(out, srcPtr - anchor, 0);
        //out.put(src, anchor, srcPtr - anchor);

        // 结束符（EndMark）
        //out.put((byte) 0);

        return Arrays.copyOf(out.array(), out.position());
    }

    private static int findHashMatch(byte[] src, int[] hashTable, int pos, boolean isLazyHash) {
        // 通过hash表快速查找匹配,返回的是最佳匹配的起始位置
        if (pos + MIN_MATCH > src.length) return -1;
        hashTotal++;
        int hash = hash(src, pos);
        int candidate = hashTable[hash];
        if(!isLazyHash) {
            hashTable[hash] = pos;
        }
        if(candidate!=-1) {
            for(int i=0;i<4;i++) {
                if(src[candidate+i]!=src[pos+i]) {
                    hashConflict++;
                    hashTable[hash] = pos;
                    return -1;
                }
            }
        }

        if (candidate != -1 && pos - candidate <= 0xFFFF) {
            return candidate;
        } else {
            hashTable[hash] = pos;
            return -1;
        }
    }

    private static int findHashPlusMatch(byte[] src, int[] hashTableShort, int[] hashTableLong, int pos) {
        // 使用改进后的hash表快速查找匹配,返回的是最佳匹配的起始位置，使用两个hash
        if (pos + MIN_MATCH > src.length) return -1;
        int hash = hash(src, pos);
        int candidate = hashTableShort[hash];
        if (candidate ==-1)
            candidate = hashTableLong[hash];
        // 判断candidate位置的匹配长度
        int matchLen = 0;
        if(candidate!=-1)
            matchLen = findMatchLength(src, candidate, pos);
        if(candidate ==-1 || pos - candidate > WINDOW_SIZE/2 || matchLen < 4) {
            hashTableShort[hash] = pos;  // 说明以前的hash的位置记录的并不是
        }
        if(matchLen >= 4){
            // 查看长度加一的hash值是否在表中，查看那个位置的匹配长度是否真的更长
            // 这里应该增加递归查找？？？？？
            int hash2 = hash(src, pos, matchLen+1);
            int candidate2 = hashTableLong[hash2];
            int matchLen2 = 0;
            if (candidate2 != -1) {
                matchLen2 = findMatchLength(src, candidate2, pos);
            }
            if(candidate2 == -1 || pos - candidate2 > WINDOW_SIZE/2 || matchLen2 < 4) {
                hashTableLong[hash2] = pos;
            }
            if(matchLen2 > matchLen) {
                candidate = candidate2;
                //System.out.println("match size:"+matchLen2);
            }
        }
        if(candidate!=-1) {
            for(int i=0;i<4;i++) {
                if(src[candidate+i]!=src[pos+i]) {
                    return -1;
                }
            }
        }

        return (candidate != -1 && pos - candidate <= 0xFFFF) ? candidate : -1;
    }

    private static int findHashMatch(byte[] src, int[] tableNear, int[] tableFar, int pos) {
        // 从两个hash表中寻找匹配位置
        if (pos + MIN_MATCH > src.length) return -1;

        int hash = hash(src, pos);
        int candidateNear = tableNear[hash];
        int candidateFar = tableFar[hash];
        if(candidateFar == -1 || pos - candidateFar > WINDOW_SIZE/2) {
            tableFar[hash] = pos;
        }
        tableNear[hash] = pos;
        if(candidateFar!=-1 && pos - candidateFar <= 0xFFFF) {
            for(int i=0;i<4;i++) {
                if(src[candidateFar+i]!=src[pos+i]) {
                    break;
                }
                if(i==3) {
                    return candidateFar;
                }
            }
        }

        if(candidateNear!=-1 && pos - candidateNear <= 0xFFFF) {
            for(int i=0;i<4;i++) {
                if(src[candidateNear+i]!=src[pos+i]) {
                    break;
                }
                if(i==3) {
                    return candidateNear;
                }
            }
        }
        return -1;
    }

    private static int findBestMatch(byte[] src, int pos) {
        // 通过便利查找最佳匹配，最佳匹配的长度小于2^16
        int maxMatchLength = 0;
        int bestPos = 0;
        int startWindow = Math.max(0, pos - WINDOW_SIZE);
        for (int j = startWindow; j < pos; j++) {
            int currentLength = 0;
            while (pos + currentLength < src.length &&
                    src[j + currentLength] == src[pos + currentLength]) {
                currentLength++;
            }
            if (currentLength > maxMatchLength) {
                maxMatchLength = currentLength;
                bestPos = j;
            }
            if(pos + maxMatchLength >= src.length) {
                break;
            }
        }
        if(maxMatchLength>=MIN_MATCH) {
            return bestPos;
        } else {
            return -1;
        }
    }

    private static void updateHashTable(byte[] src, int[] hashTable, int srcPtr, int matchLen) {
        int end = srcPtr + matchLen;
        for (int i = srcPtr; i < end; i++) {
            if (i + 3 >= src.length) {
                break; // 剩下的不足3个字节，无法构成哈希
            }
            int hash = hash(src, i);
            hashTable[hash] = i;
        }
    }

    private static void updateHashTable(byte[] src, int[] tableNear, int[] tableFar, int srcPtr, int matchLen) {
        int end = srcPtr + matchLen;
        for (int i = srcPtr; i < end; i++) {
            if (i + 3 >= src.length) {
                break; // 剩下的不足3个字节，无法构成哈希
            }
            int hash = hash(src, i);
            tableNear[hash] = i;
            if(tableFar[hash]==-1 || i - tableFar[hash] > WINDOW_SIZE/2) {
                tableFar[hash] = i;
            }
        }
    }

    private static void updateHashTablePlus(byte[] src, int[] tableShort, int[] tableLong, int srcPtr, int matchLen) {
        int end = srcPtr + matchLen;
        for (int i = srcPtr; i < end-3; i++) {  // 更新hash table short
            if (i + 3 >= src.length) {
                break; // 剩下的不足3个字节，无法构成哈希
            }
            // table short使用延迟更新的策略
            int hash = hash(src, i);
            if(tableShort[hash]==-1 || i - tableShort[hash] > WINDOW_SIZE/2) {
                tableShort[hash] = i;
            }
        }
        for(int i=srcPtr; i<end-3; i++) {   // 更新hash table long的长距离匹配
            int hash = hash(src, i, end-i+1);  // 长距离的匹配
            if(tableLong[hash]==-1 || i - tableLong[hash] > WINDOW_SIZE/2) {
                tableLong[hash] = i;
            }
        }
        // 对于xxxy,xxyy,xyyy这些情况要加入第二个hash表
        for(int i=end-3; i<end; i++) {
            if (i + 3 >= src.length) {
                break; // 剩下的不足3个字节，无法构成哈希
            }
            int hash = hash(src, i);
            if(tableLong[hash]==-1 || i - tableLong[hash] > WINDOW_SIZE/2) {
                tableLong[hash] = i;
            }
        }
    }

    static int hash(int i) {
//        return i * -1640531535 >>> 20;
        long product =(A) * (long)(i+13);
        product &= 0xFFFFFFFFL;
        return (int) (product >>> (M - N));
    }

    static int hash(int i, int temp) {
        //return i * -1640531535 >>> 20;
//        long product =(A) * (long)(i);
//        product &= 0xFFFFFFFFL;
//        int temp2 = (int)(product >>> (M - N)) + temp;
//        return temp2 & ((1 << N) - 1);
        temp <<= 8;
        temp += i;
        return hash(temp);
    }

    private static int hash(byte[] data, int pos) {
        int val = (data[pos] & 0xFF)
                | ((data[pos + 1] & 0xFF) << 8)
                | ((data[pos + 2] & 0xFF) << 16)
                | ((data[pos + 3] & 0xFF) << 24);
        int hash = hash(val);
        return hash;
    }

    private static int hash(byte[] data, int pos, int len) {
        // 对从pos开始的len个字节进行hash
//        int val = 0;
//        val = (data[pos] & 0xFF)
//                    | ((data[pos + 1] & 0xFF) << 8)
//                    | ((data[pos + 2] & 0xFF) << 16)
//                    | ((data[pos + 3] & 0xFF) << 24);
//        int hash = hash(val);
//        for(int i=4;i<len;i++) {
//            hash = hash<<8 + (data[i] & 0xFF);
//            hash = hash(hash);
//        }
        int hash = 0;
        if(pos+len<data.length) {
            hash = ((hash32.hash(data, pos, len, SEED) % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
        }
        return hash;
    }

    private static int findMatchLengthBackward(byte[] src, int matchPos, int srcPos, int minBound) {
        // 从匹配位置向前查找最长匹配
        int len = 0;
        while(src[matchPos-len]==src[srcPos-len]) {
            len++;
            if(matchPos-len<minBound){
                break;
            }
        }
        len--;
        return len;
    }

    private static int findMatchLength(byte[] src, int matchPos, int srcPos) {
        int maxLen = Math.min(src.length - srcPos, 0xFFFFFF + MIN_MATCH);
        int len = 0;
        while (len < maxLen && src[matchPos + len] == src[srcPos + len]) {
            len++;
        }
        //System.out.println("matchLen: " + len + ", maxLen: "+maxLen);
        return len >= MIN_MATCH ? len : 0;
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

    /**
     * LZ4标准解压
     *
     * @param compressed 压缩数据
     * @return 原始数据
     */
    public static byte[] decompress(byte[] compressed) {
        ByteBuffer in = ByteBuffer.wrap(compressed);
        ByteBuffer out = ByteBuffer.allocate(BLOCK_SIZE * 2);

        while (in.hasRemaining()) {
            int token = in.get() & 0xFF;
            int literalLen = token >>> 4;
            int matchLen = (token & 0x0F) + MIN_MATCH;
            //System.out.println("literalLen: " + literalLen + ", matchLen: " + matchLen);

            // 处理Literal长度扩展
            if (literalLen == 0x0F) {
                int add;
                while ((add = in.get() & 0xFF) == 0xFF) {
                    literalLen += 0xFF;
                }
                literalLen += add;
            }

            // 拷贝Literal
            for (int i = 0; i < literalLen; i++) {
                out.put(in.get());
            }

            if (!in.hasRemaining()) break;

            // 读取offset信息
            int offset = (in.get() & 0xFF) | ((in.get() & 0xFF) << 8);

            // 处理Match长度扩展
            if (matchLen == 0x0F + MIN_MATCH) {
                int add;
                while ((add = in.get() & 0xFF) == 0xFF) {
                    matchLen += 0xFF;
                }
                matchLen += add;
            }


            // 执行匹配拷贝
            int backPos = out.position() - offset;
            for (int i = 0; i < matchLen; i++) {
                out.put(out.get(backPos + i % offset));
            }
        }

        return Arrays.copyOf(out.array(), out.position());
    }

    public static byte[] decompress(byte[] compressed, int originalLength) {
        ByteBuffer in = ByteBuffer.wrap(compressed);
        ByteBuffer out = ByteBuffer.allocate(originalLength+100);

        while (in.hasRemaining()) {
            int token = in.get() & 0xFF;
            int literalLen = token >>> 4;
            int matchLen = (token & 0x0F) + MIN_MATCH;
            //System.out.println("literalLen: " + literalLen + ", matchLen: " + matchLen);

            // 处理Literal长度扩展
            if (literalLen == 0x0F) {
                int add;
                while ((add = in.get() & 0xFF) == 0xFF) {
                    literalLen += 0xFF;
                }
                literalLen += add;
            }

            // 拷贝Literal
            for (int i = 0; i < literalLen; i++) {
                out.put(in.get());
            }

            if (!in.hasRemaining()) break;

            // 读取offset信息
            int offset = (in.get() & 0xFF) | ((in.get() & 0xFF) << 8);

            // 处理Match长度扩展
            if (matchLen == 0x0F + MIN_MATCH) {
                int add;
                while ((add = in.get() & 0xFF) == 0xFF) {
                    matchLen += 0xFF;
                }
                matchLen += add;
            }


            // 执行匹配拷贝
            int backPos = out.position() - offset;
            for (int i = 0; i < matchLen; i++) {
                out.put(out.get(backPos + i % offset));
            }
        }

        return out.array();
    }
    
    public static boolean multipleCompressTest() {
        final int COMPRESS_NUM = 1000000; // 减少测试数量便于演示
        final int DATA_LEN = 50;
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
            byte[] compressed = LZ4Compress(originalData);

            // 解压
            byte[] decompressed = decompress(compressed);
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
        int[] inputArray = new int[]{11, 12, 13, 11, 12, 11, 12, 13, 11, 12, 11};
        //int[] inputArray = new int[]{41, 65, 11, 31, 42, 34, 59, 58, 35, 18, 65, 13, 42, 34, 59, 58, 87, 81, 37};
        System.out.println(Arrays.toString(inputArray));

        byte[] originalData = utils.intToBytes(inputArray);

        // 压缩
        byte[] compressed = compress(originalData, true, false);
        byte[] decompressed = decompress(compressed);
        byte[] compressed2 = LZ4Compress(originalData);
        byte[] decompressed2 = decompress(compressed2);
        System.out.println("LZ4 decompressed data:" + Arrays.toString(utils.bytesToInts(decompressed2)));
        System.out.println("compressed data:" + Arrays.toString(compressed));
        System.out.println("LZ4 compressed data:" + Arrays.toString(compressed2));
        for (byte b : compressed2) {
            System.out.println(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0') + " ");
        }

        // 解压
        //byte[] decompressed = decompress(compressed);
        if (!Arrays.equals(originalData, decompressed)) {
            System.out.println("Test failed!");
            System.out.println("Original data: " + Arrays.toString(originalData));
            return false;
        }
        return true;
    }

    // 测试代码
    public static void main(String[] args) {
        // 单次压缩解压测试
//        singleCompressTest();
//
//        // 多次压缩解压测试
//        //multipleCompressTest();
//
//        // 可与官方库交叉验证
//        // 例如: 用Python的lz4.block.compress生成的数据可在此处解压
//        int a = hash(new byte[]{-50,7,95,64}, 0);
//        int b = hash(new byte[]{92, 82,-25,46,64}, 0);
//        int c = hash(new byte[]{18, -54, -34, 64}, 0);
//        int d = hash(new byte[]{-36,-12,98,64}, 0);
//        return;
        byte[] src= {0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 3, 0, 0, 0, 2, 4, 0, 0, 0, 2, 3, 0,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11};
        byte[] res = LZ4Compress(src);
        System.out.println(res);


    }
}