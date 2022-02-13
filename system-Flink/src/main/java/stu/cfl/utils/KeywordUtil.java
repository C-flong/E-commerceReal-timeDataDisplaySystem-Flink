package stu.cfl.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    /**
     * 分词工具
     */
    public static List<String> analyze(String text){
        StringReader stringReader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);
        ArrayList<String> strings = new ArrayList<>();
        Lexeme next;
        while (true){
            try {

                if ((next=ikSegmenter.next()) != null){
                    strings.add(next.getLexemeText());
                }else {
                    break;
                }
            } catch (IOException e) {
                break;
            }
        }
        return strings;
    }
}
