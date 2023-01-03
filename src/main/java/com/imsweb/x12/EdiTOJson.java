package com.imsweb.x12;
import com.imsweb.x12.reader.X12Reader;
import com.imsweb.x12.reader.X12Reader.FileType;
import org.apache.spark.sql.api.java.UDF1;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
public class EdiToJson implements UDF1<String, String> {
    private static final long serialVersionUID = 1234567L;
    @Override
    public String call(String edi) throws Exception {
        try {
            InputStream targetStream = new ByteArrayInputStream(edi.getBytes());
            X12Reader reader = new X12Reader(FileType.ANSI837_5010_X222, targetStream);
            List<String> errors = reader.getErrors();
            List<String> errors_fatal = reader.getFatalErrors();
            List<Loop> loops = reader.getLoops();
            String json = loops.get(0).toJson();
            // Check that coordinates are
            if (json == null || json == "") {
                return null;
            } else {
                String unprettyJSON = null;
                String[] lines = json.split("[\r\n]+");
                List<String> wordList = Arrays.asList(lines);
                unprettyJSON = wordList
                        .stream()
                        .map(String::trim)
                        .reduce(String::concat)
                        .orElseThrow(FileNotFoundException::new);
                return unprettyJSON;
            }
        } catch (Exception e) {
            return "ERROR in processing EDI to JSON";
        }
    }
}