package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Statement;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

/***
 * 测试生成Statement
 *
 * 参考：TestStatementBuilder.java中代码
 *
 */
public class TestCreateStatement {

    private static final Logger log = Logger.get(TestCreateStatement.class);

    /***
     * 将sql转换成Statement
     */
    @Test
    public void createStatementBySqlParser(){

        SqlParser sqlParser = new SqlParser();
        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE /* anything */);
        String sql = "select * from foo";

        //将sql语句转成statement
        Statement statement = sqlParser.createStatement(sql, parsingOptions);
        System.out.println(statement.toString());
        System.out.print(statement.getClass().getName());

        //格式化sql
        String str = SqlFormatter.formatSql(statement, Optional.empty());
        System.out.println(str);

    }

}
