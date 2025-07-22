import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.util.*;

// 这是用来验证算法正确性的
public class LZTwoHashPlus {
    static XXHashFactory factory = XXHashFactory.fastestInstance();
    static XXHash32 hash32 = factory.hash32();
    static int seed = 0; // Hash seed
    static int MIN_MATCH = 4;
    static Vector<Integer> matchPoss = new Vector<>();
    static Vector<Integer> matchLens = new Vector<>();
    static Vector<Integer> matchOffs = new Vector<>();
    static Vector<Integer> nowPoss = new Vector<>();
    public static void compress(byte[] src) {
        MultiValuedMap<Integer, Integer> hashMap = new ArrayListValuedHashMap<>(); // 原始的Hash表V
        int srcPtr = 0;
        int anchor = 0;
        matchPoss.clear();
        matchLens.clear();
        matchOffs.clear();
        nowPoss.clear();
        while (srcPtr < src.length) {
            //System.out.println(srcPtr);
            // 查找最长匹配
            if (srcPtr == 65603) {
                System.out.println(srcPtr);
            }
            int matchPos = findHashMatch(src, hashMap, srcPtr);
            //if (matchPos != -1 && srcPtr - matchPos <= 0xFFFF && srcPtr - anchor <= 0xFFFFF) {
            if (matchPos != -1) {
                int matchLen = findMatchLength(src, matchPos, srcPtr);
                int offset = srcPtr - matchPos;
                matchLens.add(matchLen);
                matchPoss.add(matchPos);
                matchOffs.add(offset);
                nowPoss.add(srcPtr);
                // 更新Hash表
                updateHashTablePlus(src, hashMap, srcPtr, matchLen);
                srcPtr += matchLen;
                anchor = srcPtr;
            } else {
                srcPtr++;
            }
        }
    }

    private static int findHashMatch(byte[] src,  MultiValuedMap<Integer, Integer> tableNear, int srcPtr) {
        if (srcPtr + MIN_MATCH > src.length) return -1;
        int hash = hash32.hash(src, srcPtr, 4, seed);
        Collection<Integer> values = tableNear.get(hash);
        boolean found = false;
        int candidate = -1;
        for (Integer value : values) {
            if (value != -1 && src[srcPtr] == src[value] && src[srcPtr + 1] == src[value + 1] && src[srcPtr + 2] == src[value + 2] && src[srcPtr + 3] == src[value + 3]) {
                found = true;
                candidate = value;  //todo: 有可能不能直接break？
            }
        }
        if (!found) { // 没有发现，添加位置，直接退出
            tableNear.put(hash, srcPtr);
            return  -1;
        }
        // 判断candidate位置的匹配长度
        int matchLen = 0;
        int finalCandidate = -1;
        if(candidate!=-1){
            matchLen = findMatchLength(src, candidate, srcPtr);
            finalCandidate = candidate;
        }
        if(matchLen+srcPtr == src.length){
            return finalCandidate;
        }
        int nowLen = 3;
        while(true) {
            nowLen++;
            if(nowLen+srcPtr > src.length){
                return finalCandidate;
            }
            hash = hash32.hash(src, srcPtr, nowLen, seed);
            Collection<Integer> values2 = tableNear.get(hash);
            if(values2==null || values2.size()==0){
                if(nowLen>matchLen){
                    break;
                }
                continue;   // 没有找到直接结束
            }
            for (Integer value : values2) {
                if (value != -1 && findMatchLength(src, value, srcPtr) > matchLen) {
                    matchLen = findMatchLength(src, value, srcPtr);
                    finalCandidate = value;
                }
            }
        }
        return finalCandidate;
    }

    private static void updateHashTablePlus(byte[] src, MultiValuedMap<Integer, Integer> tableNear, int srcPtr, int matchLen) {
        int endPos = srcPtr + matchLen;
        if(endPos>=src.length){
            endPos = src.length-1;
        }
        int startPos = srcPtr;
        int endPosTemp = endPos;
        while (startPos < endPosTemp) {// Reverse iteration for long-distance match updates
            int hashLen = Math.max(4, endPos-startPos+1);
            if(hashLen+startPos>=src.length){return;}
            int hash = hash32.hash(src, startPos, hashLen, seed);
            Collection<Integer> values = tableNear.get(hash);
            boolean isFound = false;
            int findMatchLen = 0;
            for (Integer value : values) {
                if(findMatchLength(src, value, startPos) >= hashLen){  //todo：src改成start？
                    isFound = true;
                    findMatchLen = findMatchLength(src, value, startPos);
                    break;
                }
            }
            if(!isFound){
                tableNear.put(hash, startPos);
                startPos ++;
            } else {
                endPos = startPos + findMatchLen;  // 将结束的位置往后移
            }
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

    private static void testCSV() {
        for(int i = 0; i < CSVOperator.CSV_NUM; i++) {
            byte[] data = null;
            try {
                data = CSVOperator.getValueBytes(i);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (data.length > 1000000) {
                data = Arrays.copyOf(data, 1000000);
            }
            System.out.println(i);
            System.out.println("Data length: " + data.length);
            //data = new byte[]{4, 1, 4, 1, 4, 8, 1, 6, 1, 5, 1, 8, 2, 4, 1, 4, 1, 4, 1, 6, 10, 10, 7, 7, 9, 9, 4, 1, 4, 1, 6, 9, 8, 1, 4, 1, 5, 1, 4, 7, 4, 3, 1, 9, 5, 6, 5, 6, 9, 2};
            List<List<Object>> compressedData = LZ77.compress(data, 4, data.length);
            System.out.println("LZ77 done!");
            compress(data);
            System.out.println("Compressed done!");
            int index=0;
            for (List<Object> item : compressedData) {
                String type = (String) item.get(0);
                if ("match".equals(type)) {
                    int offset = (Integer) item.get(1);
                    int length = (Integer) item.get(2);
                    if(length != matchLens.get(index)){
                        System.out.println("now Pos: " + nowPoss.get(index));
                        System.out.println("Match pos: " + matchPoss.get(index));
                        System.out.println("Match length: " + matchLens.get(index));
                        System.out.println("LZ77 Match length: " + length);
                    }
                    index++;
                }

            }
        }
    }

    private static void testRandom() {
        int dataLen = 400;
        int ranTimes = 1000000000;
        for(int i=0; i<ranTimes; i++) {
            byte[] data = new byte[dataLen];
            Random random = new Random();
            for (int j = 0; j < dataLen; j++) {
                data[j] = (byte) (random.nextInt(5) + 1); // 生成范围为 1-10 的值
            }
            List<List<Object>> compressedData = LZ77.compress(data, 4, data.length);
            compress(data);
            System.out.println("Compressed done!");
            int index=0;
            for (List<Object> item : compressedData) {
                String type = (String) item.get(0);
                if ("match".equals(type)) {
                    int offset = (Integer) item.get(1);
                    int length = (Integer) item.get(2);
                    if(length != matchLens.get(index)){
                        System.out.println("data+"+Arrays.toString(data));
                        return;
                    }
                    index++;
                }

            }
        }
    }

    public static void main(String[] args) {
        testCSV();
        //testRandom();
    }
}
