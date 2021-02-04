package com.zlh;

import java.io.*;

/**
 * @package com.zlh
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/9/4
 */
public class FileCharsetUtil {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = new FileInputStream(new File("/Users/zlh/test_file/app5-cap6-succ_count.csv"));

        //获取文件编码
        System.out.println(getFileCharset(inputStream));
        System.out.println(getFileCharset2(new FileInputStream(new File("/Users/zlh/test_file/app5-cap6-succ_count.csv"))));
    }

    /**
     * 获取文件编码
     * @param fileInputStream
     * @return
     */
    private static String getFileCharset2(FileInputStream fileInputStream) throws IOException {
        BufferedInputStream bin = new BufferedInputStream(fileInputStream);
        int p = (bin.read() << 8) + bin.read();
        String code = null;
        //其中的 0xefbb、0xfffe、0xfeff、0x5c75这些都是这个文件的前面两个字节的16进制数
        switch (p) {
            case 0xefbb:
                code = "UTF-8";
                break;
            case 0xfffe:
                code = "Unicode";
                break;
            case 0xfeff:
                code = "UTF-16BE";
                break;
            case 0x5c75:
                code = "ANSI|ASCII" ;
                break ;
            default:
                code = "GBK";
        }

        return code;
    }

    /**
     * 获取文件编码
     * @param inputStream
     * @return
     */
    private static String getFileCharset(InputStream inputStream) {
        //默认GBK
        String charset = "GBK";
        byte[] first3Bytes = new byte[3];
        try(BufferedInputStream bis = new BufferedInputStream(inputStream)) {
            bis.mark(0);
            int read = bis.read(first3Bytes, 0, 3);
            // 文件编码为 ANSI
            if (read == -1) {
                return charset;
            }
            // 文件编码为 Unicode
            if (first3Bytes[0] == (byte) 0xFF && first3Bytes[1] == (byte) 0xFE) {
                return  "UTF-16LE";
            }
            // 文件编码为 Unicode big endian
            if (first3Bytes[0] == (byte) 0xFE && first3Bytes[1] == (byte) 0xFF) {
                return "UTF-16BE";
            }
            // 文件编码为 UTF-8
            if (first3Bytes[0] == (byte) 0xEF && first3Bytes[1] == (byte) 0xBB && first3Bytes[2] == (byte) 0xBF) {
                return "UTF-8";
            }
            bis.reset();

            while ((read = bis.read()) != -1) {
                if (read >= 0xF0) {
                    break;
                }
                // 单独出现BF以下的，也算是GBK
                if (0x80 <= read && read <= 0xBF) {
                    break;
                }
                if (0xC0 <= read && read <= 0xDF) {
                    read = bis.read();
                    // 双字节 (0xC0 - 0xDF)
                    if (0x80 <= read && read <= 0xBF) {
                        // (0x80
                        // - 0xBF),也可能在GB编码内
                        continue;
                    }
                    break;
                }
                // 也有可能出错，但是几率较小
                if (0xE0 <= read && read <= 0xEF) {
                    read = bis.read();
                    if (0x80 <= read && read <= 0xBF) {
                        read = bis.read();
                        if (0x80 <= read && read <= 0xBF) {
                            charset = "UTF-8";
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
        }
        return charset;
    }
}
