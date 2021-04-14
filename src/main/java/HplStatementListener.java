import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

public class HplStatementListener extends HplsqlBaseListener{

    private final List<String> sqlStatementConsumer;

    public HplStatementListener(List<String> sqlStatementConsumer) {
        this.sqlStatementConsumer = sqlStatementConsumer;
    }

  /*  @Override
    public void enterSemicolon_stmt(HplsqlParser.Semicolon_stmtContext ctx){
        if (ctx.getChildCount() > 0) {
           StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.enterSemicolon_stmt(ctx);
    }*/

    @Override
    public void enterProgram(HplsqlParser.ProgramContext ctx){
        if (ctx.getChildCount() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.enterProgram(ctx);
    }
 /*   @Override
    public void enterStmt(HplsqlParser.StmtContext ctx){
        if (ctx.getChildCount() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.enterStmt(ctx);
    }*/


    private void recreateStatementString(ParseTree currentNode, StringBuilder stringBuilder) {
        if (currentNode instanceof TerminalNode) {
            stringBuilder.append(currentNode.getText());
            stringBuilder.append(' ');
        }
        for (int i = 0; i < currentNode.getChildCount(); i++) {
            recreateStatementString(currentNode.getChild(i), stringBuilder);
        }
    }
}
