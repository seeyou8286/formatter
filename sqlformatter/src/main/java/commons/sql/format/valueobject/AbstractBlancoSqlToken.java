package commons.sql.format.valueobject;

public class AbstractBlancoSqlToken
{
  private int fType;
  private String fString;
  private int fPos = -1;

  public void setType(int argType)
  {
    this.fType = argType;
  }

  public int getType()
  {
    return this.fType;
  }

  public void setString(String argString)
  {
    this.fString = argString;
  }

  public String getString()
  {
    return this.fString;
  }

  public void setPos(int argPos)
  {
    this.fPos = argPos;
  }

  public int getPos()
  {
    return this.fPos;
  }

  public String toString()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("blanco.commons.sql.format.valueobject.AbstractBlancoSqlToken[");
    buf.append("type=" + this.fType);
    buf.append(",string=" + this.fString);
    buf.append(",pos=" + this.fPos);
    buf.append("]");
    return buf.toString();
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     blanco.commons.sql.format.valueobject.AbstractBlancoSqlToken
 * JD-Core Version:    0.6.2
 */