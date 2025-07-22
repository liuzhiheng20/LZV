import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LZ4Query {

    private static final int BLOCK_SIZE = 64 * 1024;
    private static final int MIN_MATCH = 4;
    private static final int HASH_LOG = 20;
    private static final int HASH_TABLE_SIZE = 4096;

    private int queryLeft = 0;
    private int queryRight = 0;
    private int findIterate = 0;
    private int findCost = 0;
    private int findSpan = 0;
    private byte[] compressData = null;
    private ByteArrayOutputStream res = null;
    public IntBuffer index2 = null;
    public IntBuffer info2 = null;
    public List<Integer> index = null; // 原始序列中，literal和match是如何间隔分布的
    public List<Integer> info = null;    // 对于literal部分，记录literral在压缩序列中的位置； 对于match部分，记录offset的长度

    public LZ4Query(byte[] compressData) {
        this.compressData = compressData;
        this.index = new ArrayList<>(100000);
        this.index2 =IntBuffer.allocate(50000);
        this.info = new ArrayList<>(100000);
        this.info2 =IntBuffer.allocate(50000);
        this.res = new ByteArrayOutputStream();
        indexCompressedData();
        //LZ4.decompress(compressData);
    }

    public LZ4Query() {
        this.compressData = null;
        this.index = new ArrayList<>();
        this.info = new ArrayList<>();
    }

    public int getFindCost() {
        return this.findIterate;
    }

    public void indexCompressedData2() {
        int len = 0;
        int i = 0;
        index2.put(len);
        while (i < compressData.length) {
            int token = compressData[i] & 0xFF;
            int literalLen = token >>> 4;
            int matchLen = (token & 0x0F) + MIN_MATCH;
            i += 1;

            if (literalLen == 0x0F) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    literalLen += 0xFF;
                    i += 1;
                }
                i += 1;
                literalLen += add;
            }
            info2.put(i);
            len += literalLen;
            i += literalLen;
            index2.put(len);

            if (i >= compressData.length-1) break;
            if (matchLen == 0x0F + MIN_MATCH) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    matchLen += 0xFF;
                    i += 1;
                }
                i += 1;
                //System.out.println("add: " + add);
                matchLen += add;
            }
            len += matchLen;
            index2.put(len);

            // 读取offset (小端序)
            int offset = (compressData[i] & 0xFF) | ((compressData[i+1] & 0xFF) << 8);
            i += 2;
            info2.put(offset);
        }

    }

    public void indexCompressedData() {
        int token;
        int literalLen = 0;
        int matchLen = 0;
        int i = 0;
        int len = 0;
        index.add(len);
        while (i < compressData.length) {
            token = compressData[i] & 0xFF;
            literalLen = token >>> 4;
            matchLen = (token & 0x0F) + 4;  // MIN_MATCH = 4
            i += 1;

            if (literalLen == 0x0F) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    literalLen += 0xFF;
                    i += 1;
                }
                i += 1;
                literalLen += add;
            }
            info.add(i); // 对于literal部分，记录literral在压缩序列中的位置
            len += literalLen;
            i += literalLen;
            index.add(len);
            //System.out.println("literal:"+len);
            if (i>=compressData.length-1) break;
            // 读取offset (小端序)
            int offset = (compressData[i] & 0xFF) | ((compressData[i+1] & 0xFF) << 8);
            i += 2;
            info.add(offset);  // 对于match部分，记录offset的长度
            if (matchLen == 0x0F + 4) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    matchLen += 0xFF;
                    i += 1;
                }
                i += 1;
                matchLen += add;
            }
            len += matchLen;
            index.add(len);
        }
        //System.out.println("index:"+index+" info:"+info);
    }

    public void indexIndexData(byte[] indexData) {
        // 从单纯的index中提取出literal和match的位置
        int token;
        int literalLen = 0;
        int matchLen = 0;
        int i = 0;
        int len = 0;
        index.add(len);
        while (i < compressData.length) {
            token = compressData[i] & 0xFF;
            literalLen = token >>> 4;
            matchLen = (token & 0x0F) + 4;  // MIN_MATCH = 4
            i += 1;

            if (literalLen == 0x0F) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    literalLen += 0xFF;
                    i += 1;
                }
                i += 1;
                literalLen += add;
            }
            info.add(i); // 对于literal部分，记录literral在压缩序列中的位置
            len += literalLen;
            //i += literalLen;
            index.add(len);
            //System.out.println("literal:"+len);
            if (i>=compressData.length-1) break;
            if (matchLen == 0x0F + 4) {
                int add;
                while ((add = compressData[i] & 0xFF) == 0xFF) {
                    matchLen += 0xFF;
                    i += 1;
                }
                i += 1;
                matchLen += add;
            }
            len += matchLen;
            index.add(len);
            // 读取offset (小端序)
            int offset = (compressData[i] & 0xFF) | ((compressData[i+1] & 0xFF) << 8);
            i += 2;
            info.add(offset);  // 对于match部分，记录offset的长度
        }
    }

    // 二分查找辅助方法
    private int findLeIndex(int target) {
        //System.out.println("target:"+target);
        // 为什么这里index会出现小于0的情况？？？
        int idx = Collections.binarySearch(index, target);
        if (idx < 0) {
            idx = -idx - 2;
        }
        if(idx >= 0) {
            int temp_1 = index.get(idx);
            int temp_2 = index.get(idx+1);
            if(temp_1 == temp_2){
                idx = idx+1;
            }
        }
        //System.out.println("index:"+idx);
        //System.out.println(ystem.out.println("index:"+idx);index.get(20773) + index.get(20774));
        return idx < 0 ? -1 : idx;
    }

    // 核心查询方法
    public void query(int left, int right) {
        if (right <= left) return;
        if (left < this.queryLeft) this.queryLeft = left;
        this.findIterate++;

        int nowpos = left;
        int leftIndex = findLeIndex(left);
        int rightIndex = findLeIndex(right-1);
//        int leftPos_2 = index.get(leftIndex-2);
//        int leftPos_1 = index.get(leftIndex-1);
//        int leftPos = index.get(leftIndex);
//        int rightPos = index.get(rightIndex+1);
        this.findCost += rightIndex - leftIndex + 1;

        //System.out.println("leftIndex: " + leftIndex + " rightIndex: " + rightIndex);
        for (int i = leftIndex; i <= rightIndex; i++) {
            // literal/ match /literal/match/...(/literal)交替分布
            int temp = info.get(i);
            if (i%2 == 0) {  // literal
                @SuppressWarnings("unchecked")
                int start = Math.max(nowpos - index.get(i), 0);
                int end = Math.min(
                        right - index.get(i),
                        index.get(i+1) - index.get(i)
                );
                //System.out.println("nowpos："+nowpos+"temp: " + temp + " start: " + start + " end: " + end);
                for (int j = start; j < end; j++) {
                    res.write(compressData[j+temp]);
                }
                nowpos = index.get(i) + end;
            }
            else { // match
                // int offset = temp ;
                // length = index[i+1] - index[i]
                // System.out.println("matchlen:"+temp);
                int matchStart = Math.max(nowpos - temp, 0);
                int matchEnd = Math.min(
                        right - temp,
                        index.get(i+1) - temp
                );

                // 递归处理匹配区域
                //System.out.println("matchStart: " + matchStart + " matchEnd: " + matchEnd + " temp: " + temp);
                query(matchStart, matchEnd);
                nowpos = index.get(i) + (matchEnd - matchStart);
            }
        }
    }

    // 首次查询入口
    public byte[] firstQuery(int left, int right) {
        resetState();
        this.queryLeft = left;
        this.queryRight = right;
        query(left, right);
        return res.toByteArray();
    }

    // 查询int32值
    public int queryInt32(int pos) {
        byte[] bytes = firstQuery(4*pos, 4*(pos+1));
        //System.out.println(Arrays.toString(bytes));
        //System.out.println("bytedata" + bytes[0]+" "+bytes[1]+" "+bytes[2]+" "+bytes[3]);
        this.findSpan = this.queryRight - this.queryLeft;
        return ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }


    public void resetState() {
        this.queryLeft = 0;
        this.queryRight = 0;
        this.findIterate = 0;
        this.findCost = 0;
        this.res.reset();
    }

}

