import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.collections4.MultiValuedMap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

public class LzVLH {
    //4.30新的算法

    private static final long A = 2654435761L; // 32-bit odd integer (golden ratio prime)
    private static final int M = 32;                // Input bit-length
    private static final int N = 21;

    private static final int HASH_TABLE_SIZE = 1 << N;
    private static final int WINDOW_SIZE = 256 * 256 * 256;
    private static final int MIN_MATCH = 4;

    static XXHashFactory factory = XXHashFactory.fastestInstance();
    static XXHash32 hash32 = factory.hash32();
    static int SEED = 0; // Hash seed
    static StreamingXXHash streamingXXHash = new StreamingXXHash(SEED);

    public static byte[] compress(byte[] src) {
        ByteBuffer out = ByteBuffer.allocate(7*src.length + src.length / 255 + 16);
        int[] hashTable = new int[HASH_TABLE_SIZE];  // 只用一个hash表
        Arrays.fill(hashTable, -1);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length) {
            // 查找最长匹配
            //System.out.println(srcPtr);
            int matchPos = findHashMatchVLH(src, hashTable, srcPtr);
            if (matchPos != -1 && srcPtr - matchPos <= 0xFFFFFF && srcPtr - anchor <= 0xFFFFFF) {
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                int offset = srcPtr - matchPos;
                if(varIntSize(offset) + 2 > matchLen) {
                    srcPtr++;
                    continue;
                }
                // 写入Literal长度（先写高4位）
                writeLiteralLength(out, srcPtr - anchor, matchLen);
                // 拷贝Literal数据
                out.put(src, anchor, srcPtr - anchor);
                // 写入Match信息
                writeMatch(out, offset, matchLen);
                // 更新Hash表
                updateHashTableVLH(src, hashTable, srcPtr, matchLen);
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

    private static int findHashMatchVLH(byte[] src, int[] hashTable, int srcPtr) {
        // 使用改进后的hash表快速查找匹配,返回的是最佳匹配的起始位置，使用两个hash
        if (srcPtr + MIN_MATCH > src.length) return -1;
        int hash = hash32.hash(src, srcPtr, 4, 0);
        hash = ((hash % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
        int candidate = hashTable[hash];
        if (candidate == -1) { // 没有发现，添加位置，直接退出
            hashTable[hash] = srcPtr;
            return -1;
        }
//        if(srcPtr - candidate >= WINDOW_SIZE/2) {
//            hashTable[hash] = srcPtr;
//        }
        // 判断candidate位置的匹配长度
        int matchLen = findMatchLength(src, candidate, srcPtr);
        int finalCandidate = candidate;
        if(matchLen+srcPtr == src.length){
            return finalCandidate;
        }
        int nowLen = 4;
        while(true) {
            nowLen++;
            if(nowLen+srcPtr > src.length){
                break;
            }
            hash = hash32.hash(src, srcPtr, nowLen, 0);
            hash = ((hash % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
            candidate = hashTable[hash];
            if(candidate == -1){
                if(nowLen>matchLen){
                    break;
                }
                continue;   // 没有找到直接结束
            }
            //if (findMatchLength(src, candidate, srcPtr) > matchLen && srcPtr - candidate < WINDOW_SIZE) {
            if (findMatchLength(src, candidate, srcPtr) >= matchLen) {
                matchLen = findMatchLength(src, candidate, srcPtr);
                finalCandidate = candidate;
            }
        }
        if(matchLen<MIN_MATCH){
            return -1;
        }
        //return (finalCandidate != -1 && srcPtr - finalCandidate <= 0xFFFF) ? finalCandidate : -1;
        return (finalCandidate != -1) ? finalCandidate : -1;
    }

    private static void updateHashTableVLH(byte[] src, int[] hashTable, int srcPtr, int matchLen) {
        int endPos = srcPtr + matchLen;
        if(endPos>=src.length){
            endPos = src.length-1;
        }
        int startPos = srcPtr;
        int endPosTemp = endPos;
        while (startPos < endPosTemp) {// Reverse iteration for long-distance match updates
            int hashLen = Math.max(4, endPos-startPos+1);
            if(hashLen+startPos>=src.length){return;}
            int hash = hash32.hash(src, startPos, hashLen, 0);
            hash = ((hash % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
            int candidate = hashTable[hash];
            boolean isFound = false;
            int findMatchLen = 0;
            if(findMatchLength(src, candidate, startPos) >= hashLen){
                isFound = true;
                findMatchLen = findMatchLength(src, candidate, startPos);
            }
            if(!isFound){
                hashTable[hash] = startPos;
                startPos ++;
            } else {
                endPos = startPos + findMatchLen;  // 将结束的位置往后移
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

    private static int hash(byte[] data, int pos, int len) {
        // 对从pos开始的len个字节进行hash
        int hash = 0;
        if(pos+len<data.length) {
            hash = ((hash32.hash(data, pos, len, SEED) % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
        }
        return hash;
    }

    private static int streamingHash(byte[] data, int pos, int len) {
        // 对从pos开始的len个字节进行hash
        int hash = 0;
        if(pos+len < data.length) {
            streamingXXHash.update(data, pos, len);
            hash = streamingXXHash.getValue();
            hash = ((hash % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
        }
        return hash;
    }

    private static int streamingHash(byte b) {
        // 对从pos开始的len个字节进行hash
        streamingXXHash.update(b);
        int hash = streamingXXHash.getValue();
        hash = ((hash % HASH_TABLE_SIZE)+HASH_TABLE_SIZE)%HASH_TABLE_SIZE;
        return hash;
    }

    private static int findMatchLength(byte[] src, int matchPos, int srcPos) {
        if(matchPos == -1)    return 0;
        int maxLen = Math.min(src.length - srcPos, 0xFFFF + MIN_MATCH);
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
        //out.put((byte) offset);
        //out.put((byte) (offset >>> 8));
        putVarInt(out, offset);
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

    public static void putVarInt(ByteBuffer out, int value) {
        while ((value & ~0x7F) != 0) {
            out.put((byte)((value & 0x7F) | 0x80));  // 设置最高位为1，表示还有字节
            value >>>= 7;
        }
        out.put((byte)(value & 0x7F));  // 最后一个字节，最高位为0
    }

    public static int varIntSize(int value) {
        int size = 0;
        do {
            size++;
            value >>>= 7;
        } while (value != 0);
        return size;
    }
}
