package com.zlh.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @package com.zlh
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/8/12
 */
public class CSVFile {
    private static final String CSV_FILE = "/Users/zlh/Documents/csvtest.csv";
//    private static final String[] FILE_HEAND = {"itemid","timestamp","value","host","ip","systemName"};
    private static final String[] FILE_HEAND = {"timestamp","value"};
    public static void main(String[] args) throws IOException {
        CSVFile csvFile = new CSVFile();
        csvFile.createCsvFile();
        List<Map<String, String>> mapList = csvFile.readCsv("/Users/zlh/Downloads/abc.csv");
        csvFile.generateCSV(mapList);
    }

    /**
     * 创建bpc bcsv文件
     * @throws SQLException
     */
    private void createCsvFile() {
        CSVPrinter csvPrinter = null;
        try {
            csvPrinter = new CSVPrinter(new FileWriter(CSV_FILE), CSVFormat.DEFAULT.withHeader(FILE_HEAND));
            csvPrinter.flush();
        } catch (IOException e) {
            System.out.println("创建bpc bcsv文件异常");
        }finally {
            try {
                csvPrinter.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * 写入csv
     * "itemid","timestamp","value","host","ip","systemName"
     * @param objectList
     */
    private void generateCSV(List<Map<String,String>> objectList) {
        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;
        try {
            writer = Files.newBufferedWriter(Paths.get(CSV_FILE)
                    //追加,会连表头一起追加
                    , StandardOpenOption.APPEND
                    ,StandardOpenOption.CREATE
            );
            //忽略表头
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withSkipHeaderRecord(true).withIgnoreEmptyLines(true));
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");
            for (Map<String,String> jsonObject:objectList){
                String timestamp = jsonObject.get("Date");
                String value = jsonObject.get("value");
                if (timestamp.endsWith(":00")){
                    String s = "{date:\""+timestamp+"\",value:"+value+"},";
                  stringBuilder.append(s);
                }
            }
            stringBuilder.append("]");
            csvPrinter.printRecord(stringBuilder.toString(),"");
            csvPrinter.flush();
        } catch (IOException e) {
            System.out.println("写入csv异常");
        }finally {
            try {
                if (csvPrinter!=null) {
                    csvPrinter.close();
                }
            } catch (IOException e) {}
            try {
                if (writer!=null) {
                    writer.close();
                }
            } catch (IOException e) {}
        }

    }

    /**
     * 读取csv数据
     * @param filePath
     * @return
     * @throws IOException
     */
    public List<Map<String, String>> readCsv(String filePath) {
        List<Map<String, String>> list = new ArrayList<>(16);
        //创建CSVFormat
        CSVFormat format = CSVFormat.DEFAULT.withHeader();
        CSVParser parser = null;
        InputStream input = null;
        try {
            File file = new File(filePath);
            input = new FileInputStream(file);
            //读取CSV数据
            parser = new CSVParser(new InputStreamReader(input),format);
            for (CSVRecord csvRecord:parser){
                Map<String, String> map = csvRecord.toMap();
                list.add(map);
            }
        } catch (IOException e) {
            System.out.println("file读取异常");
        }finally {
            if (parser != null){
                try {
                    parser.close();
                }catch (IOException e){
                }
            }
            if (input != null){
                try {
                    input.close();
                }catch (IOException e){
                }
            }
        }
        return list;
    }
}
