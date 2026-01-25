package com.opencsv.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class CharsetDetector {
    private static final int BUFFER_SIZE = 8192;
    private static final Charset[] COMMON_CHARSETS = {
            StandardCharsets.UTF_8,
            Charset.forName("GBK"),
            Charset.forName("GB18030"),
            Charset.forName("BIG5"),
            Charset.forName("BIG5-HKSCS"),
            StandardCharsets.ISO_8859_1,
            Charset.forName("ISO-8859-15"),
            Charset.forName("Windows-1252"),
            Charset.forName("Shift_JIS"),
            Charset.forName("EUC-JP"),
            Charset.forName("Windows-1251"),
            Charset.forName("KOI8-R"),
            Charset.forName("ISO-8859-5"),
            StandardCharsets.US_ASCII
    };

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

    private static final int LATIN_EXTENDED_A_START = 0x00C0;
    private static final int LATIN_EXTENDED_A_END = 0x00FF;
    private static final int LATIN_EXTENDED_B_START = 0x0100;
    private static final int LATIN_EXTENDED_B_END = 0x017F;

    public static String detectCharset(File file) throws IOException {
        return detectCharset(file, null);
    }

    public static String detectCharset(String filePath) throws IOException {
        return detectCharset(new File(filePath), null);
    }

    public static String detectCharset(File file, String defaultCharset) throws IOException {
        byte[] buffer = readFileSample(file);

        String charsetFromBOM = getCharsetFromBOM(buffer);
        if (charsetFromBOM != null) {
            return charsetFromBOM;
        }

        return detectCharsetFromBytes(buffer, defaultCharset);
    }

    private static byte[] readFileSample(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = fis.read(buffer);
            if (bytesRead < 0) {
                return new byte[0];
            }
            return Arrays.copyOf(buffer, bytesRead);
        }
    }

    private static String getCharsetFromBOM(byte[] bytes) {
        if (bytes.length >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
            return StandardCharsets.UTF_8.name();
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

            if (cjkChars > 0 && charset.equals(StandardCharsets.UTF_8)) {
                score += cjkChars;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) &&
                    (charset.name().equals("Shift_JIS") || charset.name().equals("EUC-JP"))) {
                score += (hiraganaChars + katakanaChars) * 2;
                score += cjkChars * 2;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) && charset.equals(StandardCharsets.UTF_8)) {
                score += (hiraganaChars + katakanaChars);
                score += cjkChars;
            }

            if (cyrillicChars > 0 &&
                    (charset.name().equals("Windows-1251") || charset.name().equals("KOI8-R") || charset.name().equals("ISO-8859-5"))) {
                score += cyrillicChars * 2;
            }

            if (cyrillicChars > 0 && charset.equals(StandardCharsets.UTF_8)) {
                score += cyrillicChars;
            }

            if (latinExtendedChars > 0 &&
                    (charset.equals(StandardCharsets.ISO_8859_1) || charset.name().equals("ISO-8859-15") || charset.name().equals("Windows-1252"))) {
                score += latinExtendedChars;
            }

            if (latinExtendedChars > 0 && charset.equals(StandardCharsets.UTF_8)) {
                score += latinExtendedChars / 2;
            }

            if (cyrillicChars > 0 &&
                    (charset.equals(StandardCharsets.ISO_8859_1) ||
                            charset.equals(StandardCharsets.US_ASCII) ||
                            charset.name().equals("Windows-1252") ||
                            charset.name().equals("ISO-8859-15"))) {
                score -= cyrillicChars * 5;
            }

            if (cjkChars > 0 && (charset.equals(StandardCharsets.ISO_8859_1) ||
                    charset.equals(StandardCharsets.US_ASCII) ||
                    charset.name().equals("Windows-1252") ||
                    charset.name().equals("ISO-8859-15"))) {
                score -= cjkChars * 5;
            }

            if ((hiraganaChars > 0 || katakanaChars > 0) &&
                    (charset.equals(StandardCharsets.ISO_8859_1) ||
                            charset.equals(StandardCharsets.US_ASCII) ||
                            charset.name().equals("Windows-1252") ||
                            charset.name().equals("ISO-8859-15") ||
                            charset.name().equals("GBK") ||
                            charset.name().equals("GB18030") ||
                            charset.name().equals("BIG5") ||
                            charset.name().equals("BIG5-HKSCS"))) {
                score -= (hiraganaChars + katakanaChars) * 5;
            }

            if (cyrillicChars > 0 &&
                    (charset.equals(StandardCharsets.ISO_8859_1) ||
                            charset.equals(StandardCharsets.US_ASCII) ||
                            charset.name().equals("Windows-1252") ||
                            charset.name().equals("ISO-8859-15") ||
                            charset.name().equals("GBK") ||
                            charset.name().equals("GB18030") ||
                            charset.name().equals("BIG5") ||
                            charset.name().equals("BIG5-HKSCS"))) {
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
        return (c >= LATIN_EXTENDED_A_START && c <= LATIN_EXTENDED_A_END) ||
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
