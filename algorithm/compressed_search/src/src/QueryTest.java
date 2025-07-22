import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class QueryTest {
    private static final int COMPRESS_WINDOW_SIZE = 16384;

    public static void singleQueryTest() {
        //int[] inputArray = {10, 3, 3, 5, 1, 1, 7, 2, 7, 7};

        int[] inputArray = {249, 280, 158, 86, 176, 119, 146, 31, 123, 139, 100, 284, 198, 24, 102, 234, 118, 93, 32, 93, 54, 258, 19, 96, 9, 265, 225, 245, 202, 141, 22, 55, 2, 158, 123, 138, 179, 107, 2, 131};
        byte[] byteArray = utils.intToBytes(inputArray);
        byte[] compressed = LZ4.compress(byteArray, true, false);
//        List<List<Object>> compressed = LZ77.compress(byteArray, 4, 1024);
//
        //for (int i = 0; i < inputArray.length; i++) {
            int i = 33;
            System.out.println("index:"+i+" value:"+inputArray[i]);
            LZ4Query cq = new LZ4Query(compressed);
            System.out.println("Query[" + i + "]: " + cq.queryInt32(i));
    }

    public static void singleQueryTest(int[] data) {
        int new_data[] = Arrays.copyOf(data, 80000);
        byte[] byteArray = utils.intToBytes(new_data);
        byte[] compressed = LZ4.compress(byteArray, true,false);
        int i = 79910;

        System.out.println("index:"+i+" value:"+data[i]);
        LZ4Query cq = new LZ4Query(compressed);
        System.out.println("Query[" + i + "]: " + cq.queryInt32(i));
    }

    public static boolean multipleQueryTest() {
        final int QUERY_NUM = 1000000; // 减少测试数量便于演示
        final int DATA_LEN = 50;
        Random rand = new Random();

        for (int count = 0; count < QUERY_NUM; count++) {
            //System.out.println("Test iteration: " + count);

            int[] inputArray = new int[DATA_LEN];
            for (int i = 0; i < DATA_LEN; i++) {
                inputArray[i] = rand.nextInt(51);
            }
            byte[] byteArray = utils.intToBytes(inputArray);
            //List<List<Object>> compressed = LZ77.compress(byteArray, 4, 1024);
            byte[] compressed = LZ4.compress(byteArray, true, false);
            System.err.println("Original: " + Arrays.toString(inputArray));
            for (int i = 0; i < inputArray.length; i++) {
                //LZ77Query cq = new LZ77Query(compressed);
                LZ4Query cq = new LZ4Query(compressed);
                int decompressed = cq.queryInt32(i);
                if (inputArray[i] != decompressed) {
                    System.err.println("Test failed!");
                    System.err.println("Original: " + Arrays.toString(inputArray));
                    System.err.println("Decompressed[" + i + "]: " + decompressed);
                    return false;
                }
            }
        }
        System.out.println("All tests passed!");
        return true;
    }

    private static boolean queryTimeColumnTest(byte[] deltadata, long minTime, long maxTime) {
        // 测试通过预测算法得到的序列的压缩
        return true;
    }

    private static void queryJumpNumTest(byte[] data) {
        // 用来测试不同压缩算法的压缩查询的跳转次数
        if(data.length > 800000) {
            data = Arrays.copyOf(data, 800000);
        }
        byte[] lz77compressedData = LZ4.compress(data, false, false);
        byte[] lz4compressedData = LZ4.LZ4Compress(data);
        byte[] lzTwoHashcompressedData = LZTwoHash.compress(data);
        int lz77JumpNum = 0;
        long lz77JumpNumTotal = 0;
        int lz4JumpNum = 0;
        long lz4JumpNumTotal = 0;
        int lzTwoHashJumpNum = 0;
        long lzTwoHashJumpNumTotal = 0;
        int num = data.length/8;
        for(int i = 0; i < num; i++) {
            int temp = getJumpNum(lz77compressedData, i, data);
            lz77JumpNumTotal += temp;
            if(temp > lz77JumpNum) lz77JumpNum = temp;
            temp = getJumpNum(lz4compressedData, i, data);
            lz4JumpNumTotal += temp;
            if(temp > lz4JumpNum) lz4JumpNum = temp;
            temp = getJumpNum(lzTwoHashcompressedData, i, data);
            lzTwoHashJumpNumTotal += temp;
            if(temp > lzTwoHashJumpNum) lzTwoHashJumpNum = temp;
        }
        System.out.println("LZ77 compressed jump max num: " + lz77JumpNum + " avg num: " + lz77JumpNumTotal/num);
        System.out.println("LZ4 standard compressed jump num: " + lz4JumpNum + " avg num: " + lz4JumpNumTotal/num);
        System.out.println("LZ4 two hash compressed jump num: " + lzTwoHashJumpNum + " avg num: " + lzTwoHashJumpNumTotal/num);
    }

    private static void queryValueTimeTest(byte[] data, List<List<Long>> res) {
        // 用来测试不同压缩算法的压缩查询的跳转次数
        List<Long> rowRes = new ArrayList<>();
        if(data.length > 800000) {
            data = Arrays.copyOf(data, 800000);
        }
        byte[] lz77compressedData = LZ4.compress(data, false, false);
        byte[] lz4compressedData = LZ4.LZ4Compress(data);
        byte[] lzTwoHashcompressedData = LZTwoHash.compress(data);
        long lz77QueryTime = 0;
        long lz4QueryTime = 0;
        long lzTwoHashQueryTime = 0;
        long decompressTimeStandardLZ4 = 0;
        long decompressTimeStandardLZ77 = 0;
        long decompressTimeStandardLZVLH = 0;
        //long decompressTimeOur = 0;
        int num = data.length/8;
        //for(int j=0;j<10; j++){
            for(int i = 0; i < num; i++) {
                lz77QueryTime += getQueryTime(lz77compressedData, i, data);
                lz4QueryTime += getQueryTime(lz4compressedData, i, data);
                lzTwoHashQueryTime += getQueryTime(lzTwoHashcompressedData, i, data);
                decompressTimeStandardLZ4 += getQueryTimeAllDecompress(lz4compressedData, i, data, true);
                decompressTimeStandardLZ77 += getQueryTimeAllDecompress(lz77compressedData, i, data, true);
                decompressTimeStandardLZVLH += getQueryTimeAllDecompress(lzTwoHashcompressedData, i, data, true);
                //decompressTimeOur += getQueryTimeAllDecompress(lzTwoHashcompressedData, i, data, false);
            }
        //}
        lz77QueryTime = lz77QueryTime / num;
        lz4QueryTime = lz4QueryTime / num;
        lzTwoHashQueryTime = lzTwoHashQueryTime / num;
        decompressTimeStandardLZ4 = decompressTimeStandardLZ4 / num;
        decompressTimeStandardLZ77 = decompressTimeStandardLZ77 / num;
        decompressTimeStandardLZVLH = decompressTimeStandardLZVLH / num;
        //decompressTimeOur = decompressTimeOur / num;
        System.out.println("LZ77 compressed query time: " + lz77QueryTime);
        System.out.println("LZ4 standard compressed query time: " + lz4QueryTime);
        System.out.println("LZ4 two hash compressed query time:" + lzTwoHashQueryTime);
        System.out.println("LZ4 all decompressed(LZ4) query time:" + decompressTimeStandardLZ4);
        System.out.println("LZ4 all decompressed(LZ77) query time:" + decompressTimeStandardLZ77);
        System.out.println("LZ4 all decompressed(LZVLH) query time:" + decompressTimeStandardLZVLH);
        //System.out.println("LZ4 all decompressed(our) query time:" + decompressTimeOur);
        rowRes.add(lzTwoHashQueryTime);
        rowRes.add(lz77QueryTime);
        rowRes.add(lz4QueryTime);
        rowRes.add(decompressTimeStandardLZVLH);
        rowRes.add(decompressTimeStandardLZ77);
        rowRes.add(decompressTimeStandardLZ4);
        //rowRes.add(decompressTimeOur);
        res.add(rowRes);
    }

    private static void queryTimeTimeTest(byte[] data, List<List<Long>> res, long minTime, long maxTime, byte[] originalData) {
        // 用来测试不同压缩算法的压缩查询的跳转次数
        List<Long> rowRes = new ArrayList<>();
//        if(data.length > 800000) {
//            data = Arrays.copyOf(data, 800000);
//        }
        byte[] lz77compressedData = LZ4.compress(data, false, false);
        System.out.println("lz77 compress finished");
        byte[] lz4compressedData = LZ4.LZ4Compress(data);
        byte[] lzTwoHashcompressedData = LZTwoHash.compress(data);
        long lz77QueryTime = 0;
        long lz4QueryTime = 0;
        long lzTwoHashQueryTime = 0;
        long decompressTimeStandardLZ4 = 0;
        long decompressTimeStandardLZ77 = 0;
        long decompressTimeStandardLZVLH = 0;
        //long decompressTimeOur = 0;
        int num = data.length/8;
        for(int i = 1; i < num-1; i++) {
            //System.out.println("lz77:"+i);
            lz77QueryTime += getQueryTimeTime(lz77compressedData, i, minTime, maxTime, originalData);
            //System.out.println("lz4:"+i);
            lz4QueryTime += getQueryTimeTime(lz4compressedData, i, minTime, maxTime, originalData);
            //System.out.println("lz-vlh:"+i);
            lzTwoHashQueryTime += getQueryTimeTime(lzTwoHashcompressedData, i, minTime, maxTime, originalData);
            //System.out.println("index:"+i);
            decompressTimeStandardLZ4 += getQueryTimeTimeAllDecompress(lz4compressedData, i, originalData, true, minTime, maxTime);
            decompressTimeStandardLZ77 += getQueryTimeTimeAllDecompress(lz77compressedData, i, originalData, true, minTime, maxTime);
            decompressTimeStandardLZVLH += getQueryTimeTimeAllDecompress(lzTwoHashcompressedData, i, originalData, true, minTime, maxTime);
            //decompressTimeOur += getQueryTimeTimeAllDecompress(lzTwoHashcompressedData, i, originalData, false, minTime, maxTime);
        }
        lz77QueryTime = lz77QueryTime / num;
        lz4QueryTime = lz4QueryTime / num;
        lzTwoHashQueryTime = lzTwoHashQueryTime / num;
        decompressTimeStandardLZ4 = decompressTimeStandardLZ4 / num;
        decompressTimeStandardLZ77 = decompressTimeStandardLZ77 / num;
        decompressTimeStandardLZVLH = decompressTimeStandardLZVLH / num;
        //decompressTimeOur = decompressTimeOur / num;
        System.out.println("LZ77 compressed query time: " + lz77QueryTime);
        System.out.println("LZ4 standard compressed query time: " + lz4QueryTime);
        System.out.println("LZ4 two hash compressed query time:" + lzTwoHashQueryTime);
        System.out.println("LZ4 all decompressed(lz4) query time:" + decompressTimeStandardLZ4);
        System.out.println("LZ4 all decompressed(lz77) query time:" + decompressTimeStandardLZ77);
        System.out.println("LZ4 all decompressed(lzvlh) query time:" + decompressTimeStandardLZVLH);
        //System.out.println("LZ4 all decompressed(our) query time:" + decompressTimeOur);
        rowRes.add(lzTwoHashQueryTime);
        rowRes.add(lz77QueryTime);
        rowRes.add(lz4QueryTime);
        rowRes.add(decompressTimeStandardLZVLH);
        rowRes.add(decompressTimeStandardLZ77);
        rowRes.add(decompressTimeStandardLZ4);
        //rowRes.add(decompressTimeOur);
        res.add(rowRes);
    }

    private static void queryTimeNumTest(byte[] data) {
        // 用来测试二分查找和预测查找的查找次数的区别
        if(data.length > 800000) {
            data = Arrays.copyOf(data, 800000);
        }
        byte[] lzTwoHashcompressedData = LZTwoHash.compress(data);
        int predictNum = 0;
        int binaryNum = 0;
        int preBiaNum = 0;
        int num = data.length/8;
        for(int i=0; i<100; i++) {   //随机测试的次数
            int index = new Random().nextInt(num);
            predictNum += getPredictNum(lzTwoHashcompressedData, index, data);
            binaryNum += getBinaryNum(lzTwoHashcompressedData, index, data);
            preBiaNum += getPreBinNum(lzTwoHashcompressedData, index, data);
        }
        System.out.println("LZ4 two hash compressed predict num: " + predictNum/100);
        System.out.println("LZ4 two hash compressed binary num: " + binaryNum/100);
        System.out.println("LZ4 two hash compressed binary num: " + preBiaNum/100);
    }

    private static int getJumpNum(byte[] compressedData, int index, byte[] originalData) {
        // 用来测试不同压缩算法的压缩查询的跳转次数
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        byte[]temp = cq.firstQuery(8*index, 8*(index+1));
        for (int j=0; j<temp.length; j++) {
            if (temp[j] != originalData[8*index + j]) {
                System.out.println("Error: " + index + " " + j);
                return 0;
            }
        }
        return cq.getFindCost();
    }

    private static long getQueryTime(byte[] compressedData, int index, byte[] originalData) {
        long startTime = System.nanoTime();
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        byte[]temp = cq.firstQuery(8*index, 8*(index+1));
        for (int j=0; j<temp.length; j++) {
            if (temp[j] != originalData[8*index + j]) {
                System.out.println("Error: " + index + " " + j);
                return 0;
            }
        }
        return System.nanoTime() - startTime;
    }

    private static long getQueryTimeTime(byte[] compressedData, int index, long minTime, long maxTime, byte[] originalData) {
        long startTime = System.nanoTime();
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        int getIndex = cq.compressedPredictBinaryDeltaSearch(utils.bytesToLong(Arrays.copyOfRange(originalData, 8*index, 8*(index+1))), originalData.length/8, minTime, maxTime);
        long endTime = System.nanoTime();
        for(int i=0; i<8; i++) {
            if(originalData[8*getIndex + i] != originalData[8*index + i]) {
                System.out.println("Error: " + index + " " + getIndex);
                return 0;
            }
        }
        return endTime - startTime;
    }

    private static long getQueryTimeAllDecompress(byte[] compressedData, int index, byte[] originalData, boolean isStandard) {
        long startTime = System.nanoTime();
        byte[] decompressed;
        if(isStandard) decompressed = LZ4.LZ4Decompress(compressedData, originalData.length);
        else decompressed = LZ4.decompress(compressedData);
//        for (int j=0; j<8; j++) {
//            if (decompressed[8*index + j] != originalData[8*index + j]) {
//                System.out.println("Error: " + index + " " + j);
//                return 0;
//            }
//        }
        return System.nanoTime() - startTime;
    }

    private static long getQueryTimeTimeAllDecompress(byte[] compressedData, int index, byte[] originalData, boolean isStandard, long minTime, long maxTime) {
        // 对时间列如何获取
        long startTime = System.nanoTime();
        byte[] decompressed;
        if(isStandard) decompressed = LZ4.LZ4Decompress(compressedData, originalData.length);
        else decompressed = LZ4.decompress(compressedData);
        long[] times = new long[originalData.length/8];
        times[0] = utils.bytesToLong(Arrays.copyOfRange(decompressed, 0, 8));
        int length = originalData.length/8 - 1;
        for(int i=1; i<originalData.length/8-1; i++) {
            double temp = ((double)(maxTime - minTime) * i) / length;
            times[i] = utils.bytesToLong(Arrays.copyOfRange(decompressed, 8*i, 8*(i+1))) + roundHalfToEven(minTime + temp);
        }
        int getIndex = Arrays.binarySearch(times, utils.bytesToLong(Arrays.copyOfRange(originalData, 8*index, 8*(index+1))));
        long endTime = System.nanoTime();
        for (int j=0; j<8; j++) {
            if (originalData[8*getIndex + j] != originalData[8*index + j]) {
                System.out.println("Error: " + index + " " + j);
                return 0;
            }
        }
        return endTime - startTime;
    }

    public static long roundHalfToEven(double x) {
        long floor = (long) Math.floor(x);
        double fraction = x - floor;

        if (fraction > 0.5) {
            return floor + 1;
        } else if (fraction < 0.5) {
            return floor;
        } else {
            // fraction == 0.5
            return (floor % 2 == 0) ? floor : floor + 1;
        }
    }

    private static int getPredictNum(byte[] compressedData, int index, byte[] originalData) {
        // 用来测试压缩数据时间列二分查找的查询次数
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        int num = originalData.length/8;
        long queryValue = utils.bytesToLong(Arrays.copyOfRange(originalData, 8*index, 8*(index+1)));
        int indexRes = cq.compressedPredictSearch(queryValue, num);
        if(index != indexRes) {
            if(queryValue != utils.bytesToLong(Arrays.copyOfRange(originalData, 8*indexRes, 8*(indexRes+1)))) {
                System.out.println("Error: " + index + " " + indexRes);
            }
        }
        return cq.getTimeColumnFindCost();
    }

    private static int getBinaryNum(byte[] compressedData, int index, byte[] originalData) {
        // 用来测试压缩数据时间列二分查找的查询次数
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        int num = originalData.length/8;
        long queryValue = utils.bytesToLong(Arrays.copyOfRange(originalData, 8*index, 8*(index+1)));
        int indexRes = cq.compressedBinarySearch(queryValue, num);
        if(index != indexRes) {
            if(queryValue != utils.bytesToLong(Arrays.copyOfRange(originalData, 8*indexRes, 8*(indexRes+1)))) {
                System.out.println("Error: " + index + " " + indexRes);
            }
        }
        return cq.getTimeColumnFindCost();
    }

    private static int getPreBinNum(byte[] compressedData, int index, byte[] originalData) {
        // 用来测试压缩数据时间列二分查找的查询次数
        LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
        int num = originalData.length/8;
        long queryValue = utils.bytesToLong(Arrays.copyOfRange(originalData, 8*index, 8*(index+1)));
        int indexRes = cq.compressedPredictBinarySearch(queryValue, num);
        if(index != indexRes) {
            if(queryValue != utils.bytesToLong(Arrays.copyOfRange(originalData, 8*indexRes, 8*(indexRes+1)))) {
                System.out.println("Error: " + index + " " + indexRes);
            }
        }
        return cq.getTimeColumnFindCost();
    }

    private static boolean LZ4IndexTest(int[] data) {
        return true;
    }

    public static boolean multiCSVQueryTest() {
        try {
            System.out.println("Starting CSV test...");
            CSVOperator csvOperator = new CSVOperator();
            csvOperator.initializeList() ;
            List<List<Long>> res = new ArrayList<>();
            //for(int i = 0; i < 2; i++) {
            for(int i = 0; i < csvOperator.CSV_NUM; i++) {
                List<Double> rowRes = new ArrayList<>();
                byte[] timeData = csvOperator.getTimeBytes(i);
                byte[] valueData = csvOperator.getValueBytes(i);
                byte[] timePreData = csvOperator.getValueBytes(i,2);
                long minTime = utils.bytesToLong(Arrays.copyOfRange(timeData, 0, 8));
                long maxTime = utils.bytesToLong(Arrays.copyOfRange(timeData, timeData.length-8, timeData.length));
                System.out.println("Starting CSV test...");
                if (valueData.length > 1000000) {
                    valueData = Arrays.copyOf(valueData, 1000000);
                }
                System.out.println("Data length: " + valueData.length);
                // 测试跳转次数
                //System.out.println("LZ4 compressed time(double hash): " + LZ4Compress(data, true, true));
                // 查询时间测试
                //queryTimeTest(data);
                // 测试查询的跳转次数
                //queryJumpNumTest(valueData);
                // 测试二分查找和预测查找的次数
                //queryTimeNumTest(timeData);
                // 测试对值列的压缩查询时间
                //queryValueTimeTest(valueData, res);
                // 对时间列的预测压缩查询时间
                queryTimeTimeTest(timePreData, res, minTime, maxTime, timeData);
                //break;
                csvOperator.writeLongCSV(res, true);
            }
            //csvOperator.writeLongCSV(res, true);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void queryTimeBinaryQueryTest(byte[]data, boolean isHash, boolean isTwoHash) {
        // 测试时间列的二分查找
        byte[] compressedData;
        if(isTwoHash) {
            compressedData = LZ4.compress2Hash(data);
        } else {
            compressedData = LZ4.compress(data, isHash,false);
        }
        //compressedData = LZ4.LZ4Compress(data);
        int num = data.length/4;
        System.out.println("data length:"+num);
        int queryNum = 1000;
        Random random = new Random();
        long startTime = System.currentTimeMillis();
        for(int i=0; i<queryNum; i++) {
            LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
            int queryIndex = random.nextInt(num);
            int queryValue = utils.bytesToInt(Arrays.copyOfRange(data, 4*queryIndex, 4*(queryIndex+1)));
            int index = cq.compressedPredictSearch(queryValue, num);
            if(index != queryIndex) {
                System.out.println("Error: " + queryIndex + " " + index);
                System.out.println("query value: " + queryValue + " answer value:" + utils.bytesToInt(Arrays.copyOfRange(data, 4*index, 4*(index+1))));
            }
        }
        long endTime = System.currentTimeMillis(); // 记录结束时间
        long duration = endTime - startTime; // 计算运行时长
        System.out.println("Execution time: " + duration + " milliseconds");
        startTime = System.currentTimeMillis();
        for(int i=0; i<queryNum; i++) {
            data = LZ4.decompress(compressedData);
        }
        endTime = System.currentTimeMillis(); // 记录结束时间
        duration = endTime - startTime; // 计算运行时长
        System.out.println("Execution time: " + duration + " milliseconds");
    }

    public static boolean multiTimeQueryTest() {// 测试针对于Time列的查询
        try {
            System.out.println("Starting CSV test...");

            for(int i = 0; i < CSVOperator.TIME_NUM; i++) {
                byte[] data = CSVOperator.getTimeBytes(i);
                if (data.length > 1000000) {
                    data = Arrays.copyOf(data, 1000000);
                }
                System.out.println("Data length: " + data.length);
                // 测试在时间数据上的二分查找
                queryTimeBinaryQueryTest(data, true, true);
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean singleTimeQueryTest() {
        try {
            System.out.println("Starting CSV test...");
            byte[] data = CSVOperator.getTimeBytes(2);
            if (data.length > 1000000) {
                data = Arrays.copyOf(data, 1000000);
            }
            byte[] compressedData = LZ4.compress2Hash(data);
            LZ4CompressedQuery cq = new LZ4CompressedQuery(compressedData);
            int num = data.length/4;
            int queryIndex = 112941;
            int queryValue = utils.bytesToInt(Arrays.copyOfRange(data, 4*queryIndex, 4*(queryIndex+1)));
            int index = cq.compressedPredictSearch(queryValue, num);
            if(index != queryIndex) {
                System.out.println("Error: " + queryIndex + " " + index);
                System.out.println("query value: " + queryValue + " answer value:" + utils.bytesToInt(Arrays.copyOfRange(data, 4*index, 4*(index+1))));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


    private static int[] calculateIndexSize(byte[] compressed) {
        int[] res = new int[2];
        LZ4Query cq = new LZ4Query(compressed);
        return res;
    }

    public static void main(String[] args) {
        //singleQueryTest();
        //multipleQueryTest();
        //csvQueryTest();
        multiCSVQueryTest();
        //multiTimeQueryTest();
        //singleTimeQueryTest();
    }
}