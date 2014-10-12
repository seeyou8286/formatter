package blanco.commons.sql.format.valueobject;

public class BlancoSqlToken extends AbstractBlancoSqlToken
{
  public BlancoSqlToken(int argType, String argString, int argPos)
  {
    setType(argType);
    setString(argString);
    setPos(argPos);
  }

  public BlancoSqlToken(int argType, String argString)
  {
    this(argType, argString, -1);
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.valueobject.BlancoSqlToken
 * JD-Core Version:    0.6.2
 */