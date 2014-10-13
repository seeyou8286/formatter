package commons.sql.format;

public class SqlUtil
{
  public static String replace(String argTargetString, String argFrom, String argTo)
  {
    String newStr = "";
    int lastpos = 0;
    while (true)
    {
      int pos = argTargetString.indexOf(argFrom, lastpos);
      if (pos == -1)
      {
        break;
      }
      newStr = newStr + argTargetString.substring(lastpos, pos);
      newStr = newStr + argTo;
      lastpos = pos + argFrom.length();
    }

    return newStr + argTargetString.substring(lastpos);
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.BlancoSqlUtil
 * JD-Core Version:    0.6.2
 */