import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

public class SparkListener extends SqlBaseBaseListener{


    private final List<String> sqlStatementConsumer;

    public SparkListener(List<String> sqlStatementConsumer) {
        this.sqlStatementConsumer = sqlStatementConsumer;
    }


  @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx){
        if (ctx.getChildCount() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.enterSingleStatement(ctx);
    }


    @Override
    public void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx){
        if (ctx.getChildCount() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.exitStatementDefault(ctx);
    }

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
