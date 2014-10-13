package commons.sql.format;

import java.io.IOException;

public class SqlFormatterException extends IOException
{
  public SqlFormatterException()
  {
  }

  public SqlFormatterException(String argMessage)
  {
    super(argMessage);
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.BlancoSqlFormatterException
 * JD-Core Version:    0.6.2
 */