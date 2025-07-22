import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class LZ77 {

    public static List<List<Object>> compress(byte[] inputData, int minMatchLength, int windowSize) {
        List<List<Object>> compressedData = new ArrayList<>();
        List<Byte> currentLiteral = new ArrayList<>();

        int i = 0;
        while (i < inputData.length) {
            int maxMatchLength = 0;
            int bestOffset = 0;

            int startWindow = Math.max(0, i - windowSize);
            for (int j = startWindow; j < i; j++) {
                int currentLength = 0;
                while (i + currentLength < inputData.length && inputData[j + currentLength] == inputData[i + currentLength]) {
                    currentLength++;
                }
                if (currentLength > maxMatchLength) {
                    maxMatchLength = currentLength;
                    bestOffset = i - j;
                }
            }

            if (maxMatchLength >= minMatchLength) {
                compressedData.add(Arrays.asList("literal", new ArrayList<>(currentLiteral)));
                currentLiteral.clear();
                compressedData.add(Arrays.asList("match", bestOffset, maxMatchLength));
                i += maxMatchLength;
            } else {
                currentLiteral.add(inputData[i]);
                i++;
            }
        }

        if (!currentLiteral.isEmpty()) {
            compressedData.add(Arrays.asList("literal", currentLiteral));
        }

        return compressedData;
    }

    public static byte[] decompress(List<List<Object>> compressedData) {
        List<Byte> decompressedBytes = new ArrayList<>();
        for (List<Object> item : compressedData) {
            String type = (String) item.get(0);
            if ("literal".equals(type)) {
                List<Byte> bytes = (List<Byte>) item.get(1);
                decompressedBytes.addAll(bytes);
            } else if ("match".equals(type)) {
                int offset = (Integer) item.get(1);
                int length = (Integer) item.get(2);
                int start = decompressedBytes.size() - offset;
                if (start < 0) {
                    throw new RuntimeException("Invalid offset during decompression");
                }
                for (int i = 0; i < length; i++) {
                    int copyFrom = start + i;
                    if (copyFrom >= decompressedBytes.size()) {
                        throw new RuntimeException("Invalid copy operation during decompression");
                    }
                    decompressedBytes.add(decompressedBytes.get(copyFrom));
                }
            }
        }

        byte[] byteArray = new byte[decompressedBytes.size()];
        for (int i = 0; i < byteArray.length; i++) {
            byteArray[i] = decompressedBytes.get(i);
        }
        return byteArray;
    }

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

    public static int findLeIndex(List<Integer> list, int target) {
        int idx = Collections.binarySearch(list, target);
        if (idx < 0) {
            idx = -idx - 2;
        }
        return idx < 0 ? -1 : idx;
    }

    public static boolean multipleCompressTest() {
        final int COMPRESS_NUM = 10000; // 减少测试数量便于演示
        final int DATA_LEN = 1000;
        Random rand = new Random();

        for (int count = 0; count < COMPRESS_NUM; count++) {
            System.out.println("Test iteration: " + count);

            int[] inputArray = new int[DATA_LEN];
            for (int i = 0; i < DATA_LEN; i++) {
                inputArray[i] = rand.nextInt(10001);
            }
            //System.out.println(Arrays.toString(inputArray));
            byte[] inputBytes = utils.intToBytes(inputArray);
            List<List<Object>> compressed = compress(inputBytes, 4, 1024);
            byte[] res = decompress(compressed);
            int[] decompressed = utils.bytesToInts(res);
            //System.out.println(Arrays.toString(res));

            for (int i = 0; i < inputArray.length; i++) {
                if (inputArray[i] != res[i]) {
                    System.err.println("Test failed!");
                    System.err.println("Original: " + Arrays.toString(inputArray));
                    //System.err.println("Decompressed[" + i + "]: " + decompressed);
                    return false;
                }
            }
        }
        System.out.println("All tests passed!");
        return true;
    }

    public static void singleCompressTest() {
        // Test compression and decompression
        int[] inputArray = {5, 5, 1, 2, 1, 2, 1, 3};
        byte[] inputData = utils.intToBytes(inputArray);
        List<List<Object>> compressed = compress(inputData, 4, 1024);
        System.out.println("Compressed:");
        for (List<Object> item : compressed) {
            System.out.println("  " + item);
        }

        byte[] res = decompress(compressed);
        int[] decompressed = utils.bytesToInts(res);
        System.out.print("\nDecompressed: ");
        for (int num : decompressed) {
            System.out.print(num + " ");
        }
        System.out.println();

        // Test indexing
        List<Integer> index = indexCompressedData(compressed);
        System.out.println("\nIndex: " + index);

        System.out.println("findLeIndex(index, 0): " + findLeIndex(index, 0));
        System.out.println("findLeIndex(index, 24): " + findLeIndex(index, 24));
        System.out.println("findLeIndex(index, 25): " + findLeIndex(index, 25));
        System.out.println("findLeIndex(index, 30): " + findLeIndex(index, 30));
    }

    public static void main(String[] args) {
        // singleCompressTest();
        multipleCompressTest();
    }
}
