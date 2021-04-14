import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.function.Consumer;

public class SqlStatementListener extends MySqlParserBaseListener {

    private final List<String> sqlStatementConsumer;

    public SqlStatementListener(List<String> sqlStatementConsumer) {
        this.sqlStatementConsumer = sqlStatementConsumer;
    }

    @Override
    public void enterSqlStatement(MySqlParser.SqlStatementContext ctx) {
        if (ctx.getChildCount() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            recreateStatementString(ctx.getChild(0), stringBuilder);
            stringBuilder.setCharAt(stringBuilder.length() - 1, ';');
            String recreatedSqlStatement = stringBuilder.toString();
            sqlStatementConsumer.add(recreatedSqlStatement);
        }
        super.enterSqlStatement(ctx);
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