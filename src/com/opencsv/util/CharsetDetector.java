package com.opencsv.util;

import com.opencsv.FileReader;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;

public class CharsetDetector {
    private static final int BUFFER_SIZE = 8192;
    private static final Charset[] COMMON_CHARSETS = initializeCharsets();

    private static Charset[] initializeCharsets() {
        List<Charset> charsets = new ArrayList<>();
        charsets.add(StandardCharsets.UTF_8);
        addCharsetIfSupported(charsets, "GBK");
        addCharsetIfSupported(charsets, "GB18030");
        addCharsetIfSupported(charsets, "BIG5");
        addCharsetIfSupported(charsets, "BIG5-HKSCS");
        charsets.add(StandardCharsets.ISO_8859_1);
        addCharsetIfSupported(charsets, "ISO-8859-15");
        addCharsetIfSupported(charsets, "Windows-1252");
        addCharsetIfSupported(charsets, "Shift_JIS");
        addCharsetIfSupported(charsets, "EUC-JP");
        addCharsetIfSupported(charsets, "Windows-1251");
        addCharsetIfSupported(charsets, "KOI8-R");
        addCharsetIfSupported(charsets, "ISO-8859-5");
        charsets.add(StandardCharsets.US_ASCII);
        addCharsetIfSupported(charsets, "UTF-16LE");
        addCharsetIfSupported(charsets, "UTF-16BE");
        addCharsetIfSupported(charsets, "UTF-32LE");
        addCharsetIfSupported(charsets, "UTF-32BE");
        return charsets.toArray(new Charset[0]);
    }

    private static void addCharsetIfSupported(List<Charset> charsets, String charsetName) {
        try {
            charsets.add(Charset.forName(charsetName));
        } catch (Exception e) {
            // Ignore unsupported charset
        }
    }

    private static final int CJK_START = 0x4E00;
    private static final int CJK_END = 0x9FFF;
    private static final int CJK_EXT_A_START = 0x3400;
    private static final int CJK_EXT_A_END = 0x4DBF;

    private static final int HIRAGANA_START = 0x3040;
    private static final int HIRAGANA_END = 0x309F;
    private static final int KATAKANA_START = 0x30A0;
    private static final int KATAKANA_END = 0x30FF;

    private static final int CYRILLIC_START = 0x0400;
    private static final int CYRILLIC_END = 0x04FF;

    // Latin-1 Supplement: contains common accented Latin characters (à, é, ñ, etc.)
    private static final int LATIN_1_SUPPLEMENT_START = 0x0080;
    private static final int LATIN_1_SUPPLEMENT_END = 0x00FF;
    // Latin Extended-A: additional European Latin characters
    private static final int LATIN_EXTENDED_A_START = 0x0100;
    private static final int LATIN_EXTENDED_A_END = 0x017F;
    // Latin Extended-B: additional phonetic and African Latin characters
    private static final int LATIN_EXTENDED_B_START = 0x0180;
    private static final int LATIN_EXTENDED_B_END = 0x024F;

    public static String detectCharset(File file) throws IOException {
        return detectCharset(file, null);
    }

    public static String detectCharset(String filePath) throws IOException {
        // Check if it's a file inside a compressed file
        if (FileReader.isZipEntry(filePath)) {
            // Handle zip entry
            return detectCharsetFromCompressedFile(filePath, null);
        } else if (FileReader.isCompressedFile(filePath)) {
            // Handle compressed file
            return detectCharsetFromCompressedFile(filePath, null);
        } else {
            // Handle regular file
            return detectCharset(new File(filePath), null);
        }
    }

    public static String detectCharset(File file, String defaultCharset) throws IOException {
        byte[] buffer = readFileSample(file);

        String charsetFromBOM = getCharsetFromBOM(buffer);
        if (charsetFromBOM != null) {
            // If charset detected from BOM is valid, return it directly
            if (isValidCharset(charsetFromBOM)) {
                return charsetFromBOM;
            }
        }

        return detectCharsetFromBytes(buffer, defaultCharset);
    }

    /**
     * Detect charset from compressed file
     * @param filePath file path
     * @param defaultCharset default charset
     * @return detected charset
     * @throws IOException if file does not exist or is not readable
     */
    private static String detectCharsetFromCompressedFile(String filePath, String defaultCharset) throws IOException {
        byte[] buffer = readFileSampleFromCompressedFile(filePath);

        String charsetFromBOM = getCharsetFromBOM(buffer);
        if (charsetFromBOM != null) {
            // If charset detected from BOM is valid, return it directly
            if (isValidCharset(charsetFromBOM)) {
                return charsetFromBOM;
            }
        }

        return detectCharsetFromBytes(buffer, defaultCharset);
    }

    private static boolean isValidCharset(String charsetName) {
        try {
            if (charsetName == null) return false;
            Charset.forName(charsetName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] readFileSample(File file) throws IOException {
        if (!file.exists() || !file.canRead()) {
            throw new IOException("File does not exist or is not readable: " + file.getAbsolutePath());
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = fis.read(buffer);
            if (bytesRead < 0) {
                return new byte[0];
            }
            return Arrays.copyOf(buffer, bytesRead);
        }
    }

    /**
     * Read sample data from compressed file
     * @param filePath file path
     * @return sample data
     * @throws IOException if file does not exist or is not readable
     */
    private static byte[] readFileSampleFromCompressedFile(String filePath) throws IOException {
        if (FileReader.isZipEntry(filePath)) {
            // Handle zip entry
            String zipFilePath = FileReader.getZipFilePath(filePath);
            String entryName = FileReader.getZipEntryName(filePath);

            // Check if zip file exists and is readable
            File zipFile = new File(zipFilePath);
            if (!zipFile.exists() || !zipFile.canRead()) {
                throw new IOException("Zip file does not exist or is not readable: " + zipFilePath);
            }

            try (FileInputStream fis = new FileInputStream(zipFile);
                 ZipInputStream zis = new ZipInputStream(fis)) {

                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.getName().equals(entryName)) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int bytesRead = zis.read(buffer);
                        if (bytesRead < 0) {
                            return new byte[0];
                        }
                        zis.closeEntry();
                        return Arrays.copyOf(buffer, bytesRead);
                    }
                    zis.closeEntry();
                }

                throw new IOException("Entry not found in zip file: " + entryName);
            }
        } else if (filePath.toLowerCase().endsWith(".gz")) {
            // Handle gz file
            File gzFile = new File(filePath);
            if (!gzFile.exists() || !gzFile.canRead()) {
                throw new IOException("GZ file does not exist or is not readable: " + filePath);
            }

            try (FileInputStream fis = new FileInputStream(gzFile);
                 GZIPInputStream gzis = new GZIPInputStream(fis)) {

                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead = gzis.read(buffer);
                if (bytesRead < 0) {
                    return new byte[0];
                }
                return Arrays.copyOf(buffer, bytesRead);
            }
        } else {
            // Handle regular file
            return readFileSample(new File(filePath));
        }
    }

    private static String getCharsetFromBOM(byte[] bytes) {
        // Note: Must detect UTF-32 first, because UTF-32 BOM contains UTF-16 BOM prefix
        if (bytes.length >= 4 && bytes[0] == (byte) 0xFF && bytes[1] == (byte) 0xFE && bytes[2] == (byte) 0x00 && bytes[3] == (byte) 0x00) {
            return "UTF-32LE"; // UTF-32 Little Endian BOM
        } else if (bytes.length >= 4 && bytes[0] == (byte) 0x00 && bytes[1] == (byte) 0x00 && bytes[2] == (byte) 0xFE && bytes[3] == (byte) 0xFF) {
            return "UTF-32BE"; // UTF-32 Big Endian BOM
        } else if (bytes.length >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
            return StandardCharsets.UTF_8.name(); // UTF-8 BOM
        } else if (bytes.length >= 2 && bytes[0] == (byte) 0xFF && bytes[1] == (byte) 0xFE) {
            return "UTF-16LE"; // UTF-16 Little Endian BOM
        } else if (bytes.length >= 2 && bytes[0] == (byte) 0xFE && bytes[1] == (byte) 0xFF) {
            return "UTF-16BE"; // UTF-16 Big Endian BOM
        }
        return null;
    }

    private static String detectCharsetFromBytes(byte[] bytes, String defaultCharset) {
        if (bytes.length == 0) {
            return defaultCharset != null ? defaultCharset : StandardCharsets.UTF_8.name();
        }

        String bestCharset = null;
        int bestScore = Integer.MIN_VALUE;

        for (Charset charset : COMMON_CHARSETS) {
            int score = testCharset(bytes, charset);
            if (score > bestScore) {
                bestScore = score;
                bestCharset = charset.name();
            }
        }

        if (bestCharset != null && bestScore >= 0) {
            return bestCharset;
        }

        return defaultCharset != null ? defaultCharset : StandardCharsets.UTF_8.name();
    }

    private static int testCharset(byte[] bytes, Charset charset) {
        try {
            String decoded = new String(bytes, charset);
            int score = 0;
            int totalChars = 0;
            int invalidSequences = 0;
            int cjkChars = 0;
            int hiraganaChars = 0;
            int katakanaChars = 0;
            int cyrillicChars = 0;
            int latinExtendedChars = 0;
            int asciiChars = 0;
            int controlChars = 0;

            for (int i = 0; i < decoded.length(); i++) {
                char c = decoded.charAt(i);
                totalChars++;

                if (c == '\uFFFD') {
                    invalidSequences++;
                    continue;
                }

                if (isHiragana(c)) {
                    hiraganaChars++;
                    score += 3;
                } else if (isKatakana(c)) {
                    katakanaChars++;
                    score += 3;
                } else if (isCJKCharacter(c)) {
                    cjkChars++;
                    score += 3;
                } else if (isCyrillic(c)) {
                    cyrillicChars++;
                    score += 3;
                } else if (isLatinExtended(c)) {
                    latinExtendedChars++;
                    score += 2;
                } else if (isASCIICommonChar(c)) {
                    asciiChars++;
                    score += 2;
                } else if (isASCIIPrintable(c)) {
                    asciiChars++;
                    score += 1;
                } else if (isControlChar(c)) {
                    controlChars++;
                }
            }

            if (invalidSequences > 0) {
                score -= invalidSequences * 15;
            }

            if (totalChars > 0 && invalidSequences > totalChars * 0.1) {
                return -100;
            }

            if (totalChars == 0) {
                return -100;
            }

            if (cjkChars > 0 && (charset.name().equals("GBK") || charset.name().equals("GB18030"))) {
                if (hiraganaChars == 0 && katakanaChars == 0) {
                    score += cjkChars * 2;
                } else {
                    score -= cjkChars * 3;
                }
            }

            if (cjkChars > 0 && (charset.name().equals("BIG5") || charset.name().equals("BIG5-HKSCS"))) {
                if (hiraganaChars == 0 && katakanaChars == 0 && cyrillicChars == 0) {
                    score += cjkChars * 2;
                } else {
                    score -= cjkChars * 3;
                }
            }

            if (cjkChars > 0 && charset.name().equals(StandardCharsets.UTF_8.name())) {
                score += cjkChars;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) &&
                    (charset.name().equals("Shift_JIS") || charset.name().equals("EUC-JP"))) {
                score += (hiraganaChars + katakanaChars) * 2;
                score += cjkChars * 2;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) && charset.name().equals(StandardCharsets.UTF_8.name())) {
                score += (hiraganaChars + katakanaChars);
                score += cjkChars;
            }

            if (cyrillicChars > 0 &&
                    (charset.name().equals("Windows-1251") || charset.name().equals("KOI8-R") || charset.name().equals("ISO-8859-5"))) {
                score += cyrillicChars * 2;
            }

            if (cyrillicChars > 0 && charset.name().equals(StandardCharsets.UTF_8.name())) {
                score += cyrillicChars;
            }

            if (latinExtendedChars > 0 &&
                    (charset.name().equals(StandardCharsets.ISO_8859_1.name()) || charset.name().equals("ISO-8859-15") || charset.name().equals("Windows-1252"))) {
                score += latinExtendedChars;
            }

            if (latinExtendedChars > 0 && charset.name().equals(StandardCharsets.UTF_8.name())) {
                // Use floating-point calculation and rounding to avoid precision loss
                score += (int) Math.round(latinExtendedChars / 2.0);
            }

            if (cyrillicChars > 0 &&
                    (charset.name().equals(StandardCharsets.ISO_8859_1.name()) ||
                            charset.name().equals(StandardCharsets.US_ASCII.name()) ||
                            charset.name().equals("Windows-1252") ||
                            charset.name().equals("ISO-8859-15"))) {
                score -= cyrillicChars * 5;
            }

            if (cjkChars > 0 && (charset.name().equals(StandardCharsets.ISO_8859_1.name()) ||
                    charset.name().equals(StandardCharsets.US_ASCII.name()) ||
                    charset.name().equals("Windows-1252") ||
                    charset.name().equals("ISO-8859-15"))) {
                score -= cjkChars * 5;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) &&
                    (charset.name().equals(StandardCharsets.ISO_8859_1.name()) ||
                            charset.name().equals(StandardCharsets.US_ASCII.name()) ||
                            charset.name().equals("Windows-1252") ||
                            charset.name().equals("ISO-8859-15") ||
                            charset.name().equals("GBK") ||
                            charset.name().equals("GB18030") ||
                            charset.name().equals("BIG5") ||
                            charset.name().equals("BIG5-HKSCS"))) {
                score -= (hiraganaChars + katakanaChars) * 5;
            }

            // Cyrillic characters should not appear in Chinese and Japanese encodings
            if (cyrillicChars > 0 &&
                    (charset.name().equals("GBK") ||
                            charset.name().equals("GB18030") ||
                            charset.name().equals("BIG5") ||
                            charset.name().equals("BIG5-HKSCS") ||
                            charset.name().equals("Shift_JIS") ||
                            charset.name().equals("EUC-JP"))) {
                score -= cyrillicChars * 5;
            }

            if (controlChars > totalChars * 0.3) {
                score -= controlChars * 2;
            }

            return score;
        } catch (Exception e) {
            return -100;
        }
    }

    private static boolean isCJKCharacter(char c) {
        return (c >= CJK_START && c <= CJK_END) ||
                (c >= CJK_EXT_A_START && c <= CJK_EXT_A_END);
    }

    private static boolean isHiragana(char c) {
        return c >= HIRAGANA_START && c <= HIRAGANA_END;
    }

    private static boolean isKatakana(char c) {
        return c >= KATAKANA_START && c <= KATAKANA_END;
    }

    private static boolean isCyrillic(char c) {
        return c >= CYRILLIC_START && c <= CYRILLIC_END;
    }

    private static boolean isLatinExtended(char c) {
        return (c >= LATIN_1_SUPPLEMENT_START && c <= LATIN_1_SUPPLEMENT_END) ||
                (c >= LATIN_EXTENDED_A_START && c <= LATIN_EXTENDED_A_END) ||
                (c >= LATIN_EXTENDED_B_START && c <= LATIN_EXTENDED_B_END);
    }

    private static boolean isASCIICommonChar(char c) {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') ||
                c == ' ' || c == ',' || c == '.' || c == ';' || c == ':' ||
                c == '-' || c == '_' || c == '(' || c == ')';
    }

    private static boolean isASCIIPrintable(char c) {
        return c >= 32 && c <= 126;
    }

    private static boolean isControlChar(char c) {
        return c < 32 && c != '\t' && c != '\n' && c != '\r';
    }
}
