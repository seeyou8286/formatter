package blanco.commons.sql.format;

public class SqlRule
{
  int keyword = 1;
  public static final int KEYWORD_NONE = 0;
  public static final int KEYWORD_UPPER_CASE = 1;
  public static final int KEYWORD_LOWER_CASE = 2;
  String indentString = "    ";

  private String[] fFunctionNames = null;

  public void setKeywordCase(int keyword) {
    this.keyword = keyword;
  }

  boolean isFunction(String name)
  {
    if (this.fFunctionNames == null)
      return false;
    for (int i = 0; i < this.fFunctionNames.length; i++) {
      if (this.fFunctionNames[i].equalsIgnoreCase(name))
        return true;
    }
    return false;
  }

  public void setFunctionNames(String[] names)
  {
    this.fFunctionNames = names;
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.BlancoSqlRule
 * JD-Core Version:    0.6.2
 */