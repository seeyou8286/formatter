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
