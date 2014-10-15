package commons.sql.format.valueobject;

public class SqlToken extends AbstractBlancoSqlToken
{
  public SqlToken(int argType, String argString, int argPos)
  {
    setType(argType);
    setString(argString);
    setPos(argPos);
  }

  public SqlToken(int argType, String argString)
  {
    this(argType, argString, -1);
  }
}
