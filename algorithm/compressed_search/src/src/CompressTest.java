import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.FileOutputStream;

public class CompressTest {
    private static final int REPEAT_TIMES = 10000;  // 压缩的时候重复次数是1000
    // 测试不同算法的压缩效果
    public static void main(String[] args) {
        //singleCSVCompressTest();
        //singleQueryTest();
        //LZ77test();
        //multiCSVCompressRatioTest();
        //multiCSVCompressTimeTest();
        multiCSVDECompressTimeTest();
    }

    public static double LZ4CompressHashOur(byte[] data) {
        LZ4.clear();
        byte[] compressedData = LZ4.compress2Hash(data);
        //byte[] compressedData = LZ4.compress(data, true);
        return (double) (LZ4.hashConflict)/LZ4.hashTotal;
    }

    public static byte[] LZ4CompressOur(byte[] data) {
        //byte[] compressedData = LZ4.compress2Hash(data);
        byte[] compressedData = LZ4.compress2Hash(data);
        //byte[] compressedData = LZ4.compress(data, true, true);
        byte[] decompressedData = LZ4.decompress(compressedData);
        for(int i = 0; i < data.length; i++) {
            if(data[i] != decompressedData[i]) {
                System.out.println("Error: " + i);
                break;
            }
        }
        return compressedData;
    }

    public static byte[] LZ4TwoHashCompress(byte[] data) {
        //byte[] compressedData = LZ4.compress2Hash(data);
        byte[] compressedData = LZTwoHash.compress(data);
        //byte[] compressedData = LZ4.compress(data, true, true);
        byte[] decompressedData = LZ4.LZ4Decompress(compressedData, data.length);
        //byte[] decompressedData = LZ4.decompress(compressedData);
        for(int i = 0; i < data.length; i++) {
            if(data[i] != decompressedData[i]) {
                System.out.println("Error: " + i);
                break;
            }
        }
        return compressedData;
    }

    public static long LZ4CompressTimeOur(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.compress(data, true, false);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4CompressTimeTwoHash(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZTwoHash.compress(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4DECompressTimeTwoHash(byte[] data) {
        byte[] compressedData = LZTwoHash.compress(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
           LZ4.LZ4Decompress(compressedData, data.length);
        }
        return System.nanoTime() - startTime;
    }

    public static byte[] LZ4CompressOld(byte[] data) {
        byte[] compressedData = LZ4.LZ4CompressHigh(data);
        return compressedData;
    }

    public static long LZ4CompressTimeSpeed(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.LZ4Compress(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4DECompressTimeSpeed(byte[] data) {
        byte[] compressedData = LZ4.LZ4Compress(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.LZ4Decompress(compressedData, data.length);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4CompressTimeRatio(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.LZ4CompressHigh(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4DECompressTimeRatio(byte[] data) {
        byte[] compressedData = LZ4.LZ4CompressHigh(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.LZ4DecompressHigh(compressedData, data.length);
        }
        return System.nanoTime() - startTime;
    }

    public static long lzmaCompressTime(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.lzmaCompress(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long lzmaDECompressTime(byte[] data) {
        byte[] compressedData =  LZ4.lzmaCompress(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.lzmaDecompress(compressedData);
        }
        return System.nanoTime() - startTime;
    }

    public static long snappyCompressTime(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.snappyCompress(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long snappyDECompressTime(byte[] data) {
        byte[] compressedData =  LZ4.snappyCompress(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.snappyDecompress(compressedData);
        }
        return System.nanoTime() - startTime;
    }

    public static long gzipCompressTime(byte[] data) {
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] compressedData = LZ4.gzipCompress(data);
        }
        return System.nanoTime() - startTime;
    }

    public static long gzipDECompressTime(byte[] data) {
        byte[] compressedData =  LZ4.gzipCompress(data);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.gzipDecompress(compressedData);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4DecompressTime(byte[] data) {
        byte[] compressedData = LZ4.compress(data, true, false);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.decompress(compressedData);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ4IndexTime(byte[] data) {
        byte[] compressedData = LZ4.compress(data, true, false);
        long startTime = System.nanoTime();
        for(int i = 0; i < REPEAT_TIMES; i++) {
            LZ4Query cq = new LZ4Query(compressedData);
        }
        return System.nanoTime() - startTime;
    }

    public static byte[] LZ77Compress(byte[] data) {  // LZ4中不使用hash
        byte[] compressedData = LZ4.compress(data, false, false);
        return compressedData;
    }

    public static byte[] LzVLHCompress(byte[] data) {
        byte[] compressedData = LzVLH.compress(data);
        return compressedData;
    }

    public static long LZ77CompressTime(byte[] data) {
        long startTime = System.nanoTime();
        for(int i=0; i<1; i++) {
            byte[] compressedData = LZ4.compress(data, false, false);
        }
        return System.nanoTime() - startTime;
    }

    public static long LZ77DECompressTime(byte[] data) {
        byte[] compressedData = LZ4.compress(data, false, false);
        long startTime = System.nanoTime();
        for(int i=0; i<REPEAT_TIMES; i++) {
            byte[] decompressedData = LZ4.LZ4Decompress(compressedData, data.length);
        }
        return System.nanoTime() - startTime;
    }

    public static boolean singleCSVCompressTest() {
        try {
            System.out.println("Starting CSV test...");

            // 伪代码示意，实际需要实现CSV读取
            //List<Integer> csvData = CSVOperator.readColumnByIndex("data/all_six_datasets/electricity/electricity.csv", 1);
            //List<Integer> csvData = CSVOperator.readColumnByIndex("data/all_six_datasets/electricity/electricity.csv", 1);
            //List<Integer> csvData = CSVOperator.readColumnByIndex("data/gps51/root.sg.track13.d2.csv", 1);
            List<Integer> csvData = CSVOperator.readColumnByIndex("data/ML/all_accelerometer_data_pids_13.csv", 0);
            System.out.println("Data length: " + csvData.size());
            int[] primitiveArray = csvData.stream().mapToInt(i->i).toArray();
            System.out.println(Arrays.toString(primitiveArray));            byte[] data = utils.intToBytes(primitiveArray);
            LZ4CompressOur(data);
            LZ4CompressOld(data);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static void saveToFile(byte[] data, String filename) {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            fos.write(data);
            fos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean multiCSVCompressRatioTest() {
        try {
            System.out.println("Starting CSV test...");
            CSVOperator csvOperator = new CSVOperator();
            csvOperator.initializeList() ;
            List<List<Double>> res = new ArrayList<>();
            for(int i = 0; i < csvOperator.CSV_NUM; i++) {
                List<Double> rowRes = new ArrayList<>();
                byte[] timeData = csvOperator.getTimeBytes(i);
                byte[] valueData = csvOperator.getValueBytes(i);
                byte[] timePreData = csvOperator.getValueBytes(i,2);
                if (timeData.length > 1000000) {
                    timeData = Arrays.copyOf(timeData, 1000000);  //1000000
                }
                if (valueData.length > 1000000) {
                    valueData = Arrays.copyOf(valueData, 1000000);  //1000000
                }
                if(timePreData.length > 1000000) {
                    timePreData = Arrays.copyOf(timePreData, 1000000);  //1000000
                }
                System.out.println(i);
                System.out.println("Data length: " + timeData.length);
                rowRes.add((double) timeData.length);
                rowRes.add((double) valueData.length);
                rowRes.add((double) timePreData.length);
                //saveToFile(data, "D:\\2025\\DQ\\research\\byte_data\\byte_" + i + ".bin");
                // 压缩比测试
                double timeRatio = 0;
                double valueRatio = 0;
                double timePreRatio = 0;
                //System.out.println("LZ4(two hash) compressed data length: " + (double)LZ4CompressOur(data).length/data.length);
                //System.out.println("LZ4(old) compressed data length: " + (double)LZ4CompressOld(data).length/data.length);
                timeRatio = (double)LZ4TwoHashCompress(timeData).length/timeData.length;
                valueRatio = (double)LZ4TwoHashCompress(valueData).length/valueData.length;
                timePreRatio = (double)LZ4TwoHashCompress(timePreData).length/timePreData.length;
                System.out.println("LZ4(two hash new) compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
                rowRes.add(timeRatio);
                rowRes.add(valueRatio);
                rowRes.add((double)timePreRatio);

//                timeRatio = (double)LzVLHCompress(timeData).length/timeData.length;
//                valueRatio = (double)LzVLHCompress(valueData).length/valueData.length;
//                timePreRatio = (double)LzVLHCompress(timePreData).length/timePreData.length;
//                System.out.println("LZVLH compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
//                rowRes.add(timeRatio);
//                rowRes.add(valueRatio);
//                rowRes.add(timePreRatio);

                timeRatio = (double)LZ77Compress(timeData).length/timeData.length;
                valueRatio = (double)LZ77Compress(valueData).length/valueData.length;
                timePreRatio = (double)LZ77Compress(timePreData).length/timePreData.length;
                System.out.println("LZ77 compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
                rowRes.add(timeRatio);
                rowRes.add(valueRatio);
                rowRes.add(timePreRatio);

                timeRatio = (double)LZ4.LZ4Compress(timeData).length/timeData.length;
                valueRatio = (double)LZ4.LZ4Compress(valueData).length/valueData.length;
                timePreRatio = (double)LZ4.LZ4Compress(timePreData).length/timePreData.length;
                System.out.println("LZ4(SPEED) compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
                rowRes.add(timeRatio);
                rowRes.add(valueRatio);
                rowRes.add(timePreRatio);

//                timeRatio = (double)LZ4.LZ4CompressHigh(timeData).length/timeData.length;
//                valueRatio = (double)LZ4.LZ4CompressHigh(valueData).length/valueData.length;
//                timePreRatio = (double)LZ4.LZ4CompressHigh(timePreData).length/timePreData.length;
//                System.out.println("LZ4(RATIO) compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
//                rowRes.add(timeRatio);
//                rowRes.add(valueRatio);
//                rowRes.add(timePreRatio);
//
//                timeRatio = (double)LZ4.gzipCompress(timeData).length/timeData.length;
//                valueRatio = (double)LZ4.gzipCompress(valueData).length/valueData.length;
//                timePreRatio = (double)LZ4.gzipCompress(timePreData).length/timePreData.length;
//                System.out.println("GZIP compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
//                rowRes.add(timeRatio);
//                rowRes.add(valueRatio);
//                rowRes.add(timePreRatio);
//
//                timeRatio = (double)LZ4.snappyCompress(timeData).length/timeData.length;
//                valueRatio = (double)LZ4.snappyCompress(valueData).length/valueData.length;
//                timePreRatio = (double)LZ4.snappyCompress(timePreData).length/timePreData.length;
//                System.out.println("Snappy compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
//                rowRes.add(timeRatio);
//                rowRes.add(valueRatio);
//                rowRes.add(timePreRatio);
//
//                timeRatio = (double)LZ4.lzmaCompress(timeData).length/timeData.length;
//                valueRatio = (double)LZ4.lzmaCompress(valueData).length/valueData.length;
//                timePreRatio = (double)LZ4.lzmaCompress(timePreData).length/timePreData.length;
//                System.out.println("LZMA compressed ratio: " + timeRatio + " " + valueRatio + " " + timePreRatio);
//                rowRes.add(timeRatio);
//                rowRes.add(valueRatio);
//                rowRes.add(timePreRatio);

                res.add(rowRes);
            }
            csvOperator.writeDoubleCSV(res, true);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean multiCSVCompressTimeTest() {
        try {
            System.out.println("Starting CSV test...");
            CSVOperator csvOperator = new CSVOperator();
            csvOperator.initializeList() ;
            List<List<Long>> res = new ArrayList<>();
            for(int i = 0; i < csvOperator.CSV_NUM; i++) {
                List<Long> rowRes = new ArrayList<>();
                byte[] timeData = csvOperator.getTimeBytes(i);
                byte[] valueData = csvOperator.getValueBytes(i);
                byte[] timePreData = csvOperator.getValueBytes(i,2);
                if (timeData.length > 1000000) {
                    timeData = Arrays.copyOf(timeData, 1000000);  //1000000
                }
                if (valueData.length > 1000000) {
                    valueData = Arrays.copyOf(valueData, 1000000);  //1000000
                }
                if(timePreData.length > 1000000) {
                    timePreData = Arrays.copyOf(timePreData, 1000000);  //1000000
                }
                System.out.println(i);
                System.out.println("Data length: " + timeData.length);
                rowRes.add((long)timeData.length);
                rowRes.add((long)valueData.length);
                rowRes.add((long)timePreData.length);
                //saveToFile(data, "D:\\2025\\DQ\\research\\byte_data\\byte_" + i + ".bin");
                // 压缩比测试
                long timeTime = 0;
                long valueTime = 0;
                long timePreTime = 0;
                //System.out.println("LZ4(two hash) compressed data length: " + (double)LZ4CompressOur(data).length/data.length);
                //System.out.println("LZ4(old) compressed data length: " + (double)LZ4CompressOld(data).length/data.length);
                timeTime = LZ4CompressTimeTwoHash(timeData);
                valueTime = LZ4CompressTimeTwoHash(valueData);
                timePreTime = LZ4CompressTimeTwoHash(timePreData);
                System.out.println("LZ4(two hash new) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);

                timeTime = LZ77CompressTime(timeData);
                valueTime = LZ77CompressTime(valueData);
                timePreTime = LZ77CompressTime(timePreData);
                System.out.println("LZ77 compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);
//
                timeTime = LZ4CompressTimeSpeed(timeData);
                valueTime = LZ4CompressTimeSpeed(valueData);
                timePreTime = LZ4CompressTimeSpeed(timePreData);
                System.out.println("LZ4(SPEED) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);

//                timeTime = LZ4CompressTimeRatio(timeData);
//                valueTime = LZ4CompressTimeRatio(valueData);
//                timePreTime = LZ4CompressTimeRatio(timePreData);
//                System.out.println("LZ4(RATIO) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);
//
//                timeTime = gzipCompressTime(timeData);
//                valueTime = gzipCompressTime(valueData);
//                timePreTime = gzipCompressTime(timePreData);
//                System.out.println("GZIP compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);
//
//                timeTime = snappyCompressTime(timeData);
//                valueTime = snappyCompressTime(valueData);
//                timePreTime = snappyCompressTime(timePreData);
//                System.out.println("Snappy compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);
//
//                timeTime = lzmaCompressTime(timeData);
//                valueTime = lzmaCompressTime(valueData);
//                timePreTime = lzmaCompressTime(timePreData);
//                System.out.println("LZMA compressed ratio: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);

                res.add(rowRes);
            }
            csvOperator.writeLongCSV(res, true);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean multiCSVDECompressTimeTest() {
        try {
            System.out.println("Starting CSV test...");
            CSVOperator csvOperator = new CSVOperator();
            csvOperator.initializeList() ;
            List<List<Long>> res = new ArrayList<>();
            for(int i = 0; i < csvOperator.CSV_NUM; i++) {
                List<Long> rowRes = new ArrayList<>();
                byte[] timeData = csvOperator.getTimeBytes(i);
                byte[] valueData = csvOperator.getValueBytes(i);
                byte[] timePreData = csvOperator.getValueBytes(i,2);
                if (timeData.length > 1000000) {
                    timeData = Arrays.copyOf(timeData, 1000000);  //1000000
                }
                if (valueData.length > 1000000) {
                    valueData = Arrays.copyOf(valueData, 1000000);  //1000000
                }
                if(timePreData.length > 1000000) {
                    timePreData = Arrays.copyOf(timePreData, 1000000);  //1000000
                }
                System.out.println(i);
                System.out.println("Data length: " + timeData.length);
                rowRes.add((long)timeData.length);
                rowRes.add((long)valueData.length);
                rowRes.add((long)timePreData.length);
                //saveToFile(data, "D:\\2025\\DQ\\research\\byte_data\\byte_" + i + ".bin");
                // 压缩比测试
                long timeTime = 0;
                long valueTime = 0;
                long timePreTime = 0;
                //System.out.println("LZ4(two hash) compressed data length: " + (double)LZ4CompressOur(data).length/data.length);
                //System.out.println("LZ4(old) compressed data length: " + (double)LZ4CompressOld(data).length/data.length);

                timeTime = LZ77DECompressTime(timeData);
                valueTime = LZ77DECompressTime(valueData);
                timePreTime = LZ77DECompressTime(timePreData);
                System.out.println("LZ77 compressed time: " + timeTime + " " + valueTime + " " + timePreTime);

                timeTime = LZ4DECompressTimeTwoHash(timeData);
                valueTime = LZ4DECompressTimeTwoHash(valueData);
                timePreTime = LZ4DECompressTimeTwoHash(timePreData);
                System.out.println("LZ4(two hash new) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);

                timeTime = LZ77DECompressTime(timeData);
                valueTime = LZ77DECompressTime(valueData);
                timePreTime = LZ77DECompressTime(timePreData);
                System.out.println("LZ77 compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);

                timeTime = LZ4DECompressTimeSpeed(timeData);
                valueTime = LZ4DECompressTimeSpeed(valueData);
                timePreTime = LZ4DECompressTimeSpeed(timePreData);
                System.out.println("LZ4(SPEED) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
                rowRes.add(timeTime);
                rowRes.add(valueTime);
                rowRes.add(timePreTime);

//                timeTime = LZ4DECompressTimeRatio(timeData);
//                valueTime = LZ4DECompressTimeRatio(valueData);
//                timePreTime = LZ4DECompressTimeRatio(timePreData);
//                System.out.println("LZ4(RATIO) compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);

//                timeTime = gzipDECompressTime(timeData);
//                valueTime = gzipDECompressTime(valueData);
//                timePreTime = gzipDECompressTime(timePreData);
//                System.out.println("GZIP compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);
//
//                timeTime = snappyDECompressTime(timeData);
//                valueTime = snappyDECompressTime(valueData);
//                timePreTime = snappyDECompressTime(timePreData);
//                System.out.println("Snappy compressed time: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);
//
//                timeTime = lzmaDECompressTime(timeData);
//                valueTime = lzmaDECompressTime(valueData);
//                timePreTime = lzmaDECompressTime(timePreData);
//                System.out.println("LZMA compressed ratio: " + timeTime + " " + valueTime + " " + timePreTime);
//                rowRes.add(timeTime);
//                rowRes.add(valueTime);
//                rowRes.add(timePreTime);

                res.add(rowRes);
            }
            csvOperator.writeLongCSV(res, true);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
