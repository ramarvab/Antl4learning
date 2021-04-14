import com.google.common.io.CharStreams;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

public class test1 {
    public void mySqlStatementsToConsumer(String sourceCode, List<String> mySqlStatementConsumer) throws ParseException {
        //CharStream codePointCharStream = CharStreams.fromString(sourceCode);
        //MySqlLexer tokenSource = new MySqlLexer(new CaseChangingCharStream(codePointCharStream, true));
        //TokenStream tokenStream = new CommonTokenStream(tokenSource);
        //MySqlParser mySqlParser = new MySqlParser(tokenStream);a
        ANTLRInputStream input = new ANTLRInputStream(sourceCode);
        HplsqlLexer lexer1 = new HplsqlLexer(input);
        //MySqlLexer lexer = new MySqlLexer(input);
        HplsqlParser hplsqlParser = new HplsqlParser(new CommonTokenStream(lexer1));
        HplStatementListener hplStatementListener = new HplStatementListener(mySqlStatementConsumer);
        //MySqlParser mySqlParser = new MySqlParser(new CommonTokenStream(lexer));
        //SqlStatementListener statementListener = new SqlStatementListener(mySqlStatementConsumer);
        //ParseTreeWalker.DEFAULT.walk(statementListener, mySqlParser.sqlStatements());
        ParseTreeWalker.DEFAULT.walk(hplStatementListener, hplsqlParser.program());



    }
    public static void main(String[] args) throws ParseException {
        /*String sql="select cust_name from database..table where cust_name like 'Kash%'";

        MySqlLexer mySqlLexer = new MySqlLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(mySqlLexer);
        MySqlParser mySqlParser = new MySqlParser(tokens);

        ParseTree tree = mySqlParser.dmlStatement();
        ParseTreeWalker walker = new ParseTreeWalker();
        MySqlParserBaseListener listener=new MySqlParserBaseListener();
        ParseTreeWalker.DEFAULT.walk(listener, tree);
        System.out.println("hi");*/

       /* String sql = "SELECT CUST_NAME FROM CUSTOMERS WHERE CUST_NAME LIKE 'Kash%'; select a.id as u_id from userT a where a.id = abc".toUpperCase(Locale.ROOT);
        ANTLRInputStream input = new ANTLRInputStream(sql);
        MySqlLexer lexer = new MySqlLexer(input);
        MySqlParser parser = new MySqlParser(new CommonTokenStream(lexer));
        ParseTree root = parser.sqlStatements();
        ParseTreeWalker walker = new ParseTreeWalker();
        MySqlParserBaseListener listener=new MySqlParserBaseListener();
        ParseTreeWalker.DEFAULT.walk(listener, root);
        //System.out.println(listener.);
*/
        String sql = "\n" +
                "create table sbg_published.es_lic_with_high_revenue2 as\n" +
                "select\n" +
                "       application_licensenumber\n" +
                "       ,dbRevenueInLastYear\n" +
                "       ,qbf_dbrevenueinpreviouscalendaryear as rev_last_clndr_yr\n" +
                "       ,qbversion\n" +
                "       , translate(a.application_licensenumber,'-','') as license_nbr\n" +
                "       , count_companyguid\n" +
                "       , event_date\n" +
                "       , uploadtime\n" +
                "\n" +
                " from sbg_published.tern_fact_ipd_license_daily a\n" +
                "     left outer join sbg_published.qbdt_educational_licenses b\n" +
                "on  translate(a.application_licensenumber,'-','')=b.lic_nbr\n" +
                "where b.lic_nbr is null\n" +
                "and qbversion like '%Enterprise%'\n" +
                "##and dbRevenueInLastYear > 5000000\n" +
                "and event_date >= CAST('2018-01-01' AS DATE)\n" .toUpperCase();        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(sql);
        System.out.println(tree.dump());
        //System.out.println(root.toStringTree(parser));

    }
}
