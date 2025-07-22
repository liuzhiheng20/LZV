package org.apache.tsfile.compress;

import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Lz77Compressor implements ICompressor{
    private static final int MIN_MATCH = 4;
    private static final int WINDOW_SIZE = 256 * 256-2;

    public static int lz77Compress(byte[] src, byte[] compressed) {
        ByteBuffer out = ByteBuffer.wrap(compressed);

        int srcPtr = 0;
        int anchor = 0;

        while (srcPtr < src.length) {
            // 查找最长匹配
            int matchPos = findBestMatch(src, srcPtr);
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
                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }

        // 处理最后的Literal
        writeLiteralLength(out, srcPtr - anchor, 0);
        out.put(src, anchor, srcPtr - anchor);
        return out.position();
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


    @Override
    public byte[] compress(byte[] data) throws IOException {
        return new byte[0];
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        return new byte[0];
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
        if (data.length > length) {
            data = Arrays.copyOfRange(data, offset, offset + length);
        }
        return lz77Compress(data, compressed);
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
        return 0;
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
        return uncompressedDataSize + 10;
    }

    @Override
    public CompressionType getType() {
        return CompressionType.LZ77;
    }
}
