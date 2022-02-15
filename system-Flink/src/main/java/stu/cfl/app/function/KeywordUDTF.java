package stu.cfl.app.function;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import stu.cfl.utils.KeywordUtil;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    /**
     * 分词UDTF函数
     * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/functions/udfs.html#implementation-guide
     */
    public void eval(String str) {
        List<String> analyze = KeywordUtil.analyze(str);

        for (String s : analyze) {
            // use collect(...) to emit a row
            // collect(Row.of(s, s.length()));
            collect(Row.of(s));
        }
    }
}
