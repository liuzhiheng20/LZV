import java.util.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.io.ByteArrayOutputStream;

public class LZ77Query {
    private int queryLeft;
    private int queryRight;
    private final List<List<Object>> compressData;
    private final ByteArrayOutputStream res = new ByteArrayOutputStream();
    private final List<Integer> index;
    private int findCost;
    private int findIterate;
    private int findSpan;

    public LZ77Query(List<List<Object>> compressData) {
        this.compressData = compressData;
        this.index = indexCompressedData(compressData);
    }

    // 帮助方法：构建索引
    public static List<Integer> indexCompressedData(List<List<Object>> compressedData) {
        List<Integer> index = new ArrayList<>();
        int currentPosition = 0;
        index.add(currentPosition);
        for (List<Object> item : compressedData) {
            String type = (String) item.get(0);
            if ("literal".equals(type)) {
                List<Byte> bytes = (List<Byte>) item.get(1);
                currentPosition += bytes.size();
            } else if ("match".equals(type)) {
                int length = (Integer) item.get(2);
                currentPosition += length;
            }
            index.add(currentPosition);
        }
        return index;
    }

    // 二分查找辅助方法
    private int findLeIndex(int target) {
        int idx = Collections.binarySearch(index, target);
        if (idx < 0) {
            idx = -idx - 2;
        }
        return idx < 0 ? -1 : idx;
    }

    // 核心查询方法
    public void query(int left, int right) {
        if (right <= left) return;
        if (left < this.queryLeft) this.queryLeft = left;
        this.findIterate++;

        int nowpos = left;
        int leftIndex = findLeIndex(left);
        int rightIndex = findLeIndex(right);
        this.findCost += rightIndex - leftIndex + 1;

        for (int i = leftIndex; i <= Math.min(rightIndex, compressData.size()-1); i++) {
            List<Object> item = compressData.get(i);
            String type = (String) item.get(0);

            if (type.equals("literal")) {
                @SuppressWarnings("unchecked")
                List<Byte> bytes = (List<Byte>) item.get(1);
                int start = Math.max(nowpos - index.get(i), 0);
                int end = Math.min(
                        right - index.get(i),
                        bytes.size()
                );

                for (int j = start; j < end; j++) {
                    res.write(bytes.get(j));
                }
                nowpos = index.get(i) + end;
            }
            else if (type.equals("match")) {
                int offset = (Integer) item.get(1);
                int length = (Integer) item.get(2);
                int matchStart = Math.max(nowpos - offset, 0);
                int matchEnd = Math.min(
                        right - offset,
                        index.get(i+1) - offset
                );

                // 递归处理匹配区域
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
        this.findSpan = this.queryRight - this.queryLeft;
        return ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    // 查询double值
    public double queryFloat64(int pos) {
        byte[] bytes = firstQuery(8*pos, 8*(pos+1));
        return ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .getDouble();
    }

    // 重置状态用于新查询
    private void resetState() {
        res.reset();
        findCost = 0;
        findIterate = 0;
        findSpan = 0;
    }

    // 获取统计指标
//    public Map<String, Integer> getMetrics() {
//        return Map.of(
//                "find_cost", findCost,
//                "find_iterate", findIterate,
//                "find_span", findSpan
//        );
//    }

    public static void main(String[] args) {
        // 测试用例
        // int[] data = new int[]{1, 2, 1, 2, 1, 2, 3, 4};
        int[] data = new int[]{7248, 8056, 3030, 9858, 5310, 3202, 5334, 9758, 5029, 4233};
        byte[] dataBytes = utils.intToBytes(data);
        List<List<Object>> compressed = LZ77.compress(dataBytes, 4, 1024);

        LZ77Query cq = new LZ77Query(compressed);
        System.out.println("index:");

        // 测试整型查询
        for(int i=0; i<data.length; i++) {
            System.out.println("Query int at position 1: " + cq.queryInt32(i));
        }
        //System.out.println("Metrics: " + cq.getMetrics());

        // 测试范围查询
        byte[] result = cq.firstQuery(4, 8);
        System.out.println("Range query result: " + Arrays.toString(result));
    }
}