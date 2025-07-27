import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.nio.charset.Charset;

public class CSVOperator {
    private static final List<String> pathList = new ArrayList<>();
    private static final List<Integer> columnList = new ArrayList<>();
    private static final List<String> timePathList = new ArrayList<>();
    private static final List<Integer> timeColumnList = new ArrayList<>();

    public static int CSV_NUM = 0;
    public static int TIME_NUM = timePathList.size();

    public static void initializeList() {
        String content = null;
        try {
            content = new String(Files.readAllBytes(Paths.get("..\\..\\data_LZV\\config.json")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 简单查找匹配
        String[] lines = content.split("\\{");
        for (String line : lines) {
            if (line.contains("\"path\"")) {
                String path = line.split("\"path\"\\s*:\\s*\"")[1].split("\"")[0];
                String columnStr = line.split("\"column\"\\s*:\\s*")[1].split("}")[0].trim();
                int column = Integer.parseInt(columnStr.replace(",", ""));
                System.out.println("Path: " + path + ", Column: " + column);
                pathList.add(path);
                columnList.add(column);
                CSV_NUM++;
            }
        }
    }

    /**
     * 读取CSV文件中指定列的数据（根据列索引）
     * @param filePath   CSV文件路径
     * @param columnIndex 要读取的列索引
     * @return 包含整型数据的列表
     */
    public static List<Integer> readColumnByIndex(String filePath, int columnIndex) throws IOException {
        List<Integer> columnData = new ArrayList<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath), Charset.forName("ISO-8859-1"))) {
            // 跳过第一行标题
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length > columnIndex) {
                    try {
                        double value = Double.parseDouble(tokens[columnIndex]);
                        int value2 = (int) value;
                        columnData.add(value2);
                    } catch (NumberFormatException e) {
                        throw new IOException("解析错误 [" + tokens[columnIndex] + "] in line: " + line);
                    }
                }
            }
        }
        return columnData;
    }

    public static byte[] readBytesByFileIndex(String filePath, int columnIndex) throws IOException {
        List<Number> columnData = new ArrayList<>();
        filePath = "..\\..\\data_LZV\\"+filePath;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath), Charset.forName("ISO-8859-1"))) {
            // Skip the first line (header)
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length > columnIndex) {
                    try {
                        Number value = determineType(tokens[columnIndex]);
                        columnData.add(value);
                    } catch (NumberFormatException e) {
                        throw new IOException("Parsing error [" + tokens[columnIndex] + "] in line: " + line);
                    }
                }
            }
        }
        System.out.println("Type: " + columnData.get(columnIndex).getClass().getSimpleName());
        Number[] primitiveArray = new Number[columnData.size()];
        columnData.toArray(primitiveArray);
        return utils.numberToBytes(primitiveArray);
    }

    public static Number determineType(String token) {   // 将数据分为long和double两种类型
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e1) {
            try {
                return (long)(Double.parseDouble(token));
            } catch (NumberFormatException e2) {
                throw new NumberFormatException("Unable to parse token: " + token);
            }
        }
    }

    public static List<Long> readTimeColumn(String filePath, int columnIndex) throws IOException {
        List<Long> columnData = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            // 跳过第一行标题
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length > columnIndex) {
                    try {
                        columnData.add(convertStrToLong(tokens[columnIndex]));
                    } catch (NumberFormatException e) {
                        throw new IOException("解析错误 [" + tokens[columnIndex] + "] in line: " + line);
                    }
                }
            }
        }
        return columnData;
    }

    public static long convertStrToLong(String timeStr) {
        // 定义时间格式器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 解析字符串为 LocalDateTime（无时区）
        LocalDateTime localDateTime = LocalDateTime.parse(timeStr, formatter);

        // 添加时区信息后转为时间戳（假设时间是系统默认时区）
        long timestamp = localDateTime
                .atZone(ZoneId.systemDefault())  // 按需替换特定时区，如 ZoneId.of("UTC")
                .toInstant()
                .toEpochMilli();

        return timestamp;
    }

    public static byte[] getValueBytes(int index) throws IOException {
        // string的list
        return readBytesByFileIndex(pathList.get(index), columnList.get(index));
    }

    public static byte[] getValueBytes(int index, int columnIndex) throws IOException {
        // string的list
        return readBytesByFileIndex(pathList.get(index), columnIndex);
    }

    public static byte[] getTimeBytes(int index) throws IOException {
        // string的list
        return readBytesByFileIndex(pathList.get(index), 0);
    }

    public static void writeDoubleCSV(List<List<Double>> data, boolean withBom) throws IOException {
        String filename = "data/output.csv";  // 输出文件名
        try (
                OutputStreamWriter writer = new OutputStreamWriter(
                        new FileOutputStream(filename),
                        StandardCharsets.UTF_8
                );
                BufferedWriter bw = new BufferedWriter(writer)
        ) {
            if (withBom) {
                bw.write('\uFEFF');  // 写入 UTF-8 BOM
            }

            for (List<Double> row : data) {
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < row.size(); i++) {
                    if (i > 0) line.append(",");
                    Double value = row.get(i);
                    line.append(value != null ? value.toString() : "");
                }
                bw.write(line.toString());
                bw.newLine();
            }
        }
    }

    public static void writeLongCSV(List<List<Long>> data, boolean withBom) throws IOException {
        String filename = System.getProperty("user.dir") + "\\res\\compress_res.csv";
        try (
                OutputStreamWriter writer = new OutputStreamWriter(
                        new FileOutputStream(filename),
                        StandardCharsets.UTF_8
                );
                BufferedWriter bw = new BufferedWriter(writer)
        ) {
            if (withBom) {
                bw.write('\uFEFF');  // 写入 UTF-8 BOM 头，防止中文乱码（尤其在 Excel 中）
            }

            for (List<Long> row : data) {
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < row.size(); i++) {
                    if (i > 0) line.append(",");
                    Long value = row.get(i);
                    line.append(value != null ? value.toString() : "");
                }
                bw.write(line.toString());
                bw.newLine();
            }
        }
    }
}