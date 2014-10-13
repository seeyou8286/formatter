package blanco.commons.sql.format.valueobject;

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

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.valueobject.BlancoSqlToken
 * JD-Core Version:    0.6.2
 */