package commons.sql.format;

import java.util.ArrayList;
import java.util.List;

import commons.sql.format.valueobject.SqlToken;

public class SqlParser
{
  private String fBefore;
  private char fChar;
  private int fPos;
  private static final String[] twoCharacterSymbol = { "<>", "<=", ">=", "||" };

  public static boolean isSpace(char argChar)
  {
    return (argChar == ' ') || (argChar == '\t') || (argChar == '\n') || (argChar == '\r') || (argChar == 65535);
  }

  public static boolean isLetter(char argChar)
  {
    if (isSpace(argChar)) {
      return false;
    }
    if (isDigit(argChar)) {
      return false;
    }
    if (isSymbol(argChar)) {
      return false;
    }
    return true;
  }

  public static boolean isDigit(char argChar)
  {
    return ('0' <= argChar) && (argChar <= '9');
  }

  public static boolean isSymbol(char argChar)
  {
    switch (argChar)
    {
    case '"':
    case '%':
    case '&':
    case '\'':
    case '(':
    case ')':
    case '*':
    case '+':
    case ',':
    case '-':
    case '.':
    case '/':
    case ':':
    case ';':
    case '<':
    case '=':
    case '>':
    case '?':
    case '|':
      return true;
    }
    return false;
  }

  SqlToken nextToken()
  {
    int start_pos = this.fPos;
    if (this.fPos >= this.fBefore.length()) {
      this.fPos += 1;
      return new SqlToken(6, "", start_pos);
    }

    this.fChar = this.fBefore.charAt(this.fPos);

    if (isSpace(this.fChar)) {
      String workString = "";
      do {
        workString = workString + this.fChar;
        this.fChar = this.fBefore.charAt(this.fPos);
        if (!isSpace(this.fChar)) {
          return new SqlToken(0, workString, start_pos);
        }

        this.fPos += 1;
      }while (this.fPos < this.fBefore.length());
      return new SqlToken(0, workString, start_pos);
    }

    if (this.fChar == ';') {
      this.fPos += 1;

      return new SqlToken(1, ";", start_pos);
    }
    if (isDigit(this.fChar)) {
      String s = "";
      while ((isDigit(this.fChar)) || (this.fChar == '.'))
      {
        s = s + this.fChar;
        this.fPos += 1;

        if (this.fPos >= this.fBefore.length())
        {
          break;
        }

        this.fChar = this.fBefore.charAt(this.fPos);
      }
      return new SqlToken(4, s, start_pos);
    }
    if (isLetter(this.fChar)) {
      String s = "";

      while ((isLetter(this.fChar)) || (isDigit(this.fChar)) || (this.fChar == '.')) {
        s = s + this.fChar;
        this.fPos += 1;
        if (this.fPos >= this.fBefore.length())
        {
          break;
        }
        this.fChar = this.fBefore.charAt(this.fPos);
      }
      for (int i = 0; i < SqlConstants.SQL_RESERVED_WORDS.length; i++) {
        if (s.compareToIgnoreCase(SqlConstants.SQL_RESERVED_WORDS[i]) == 0)
        {
          return new SqlToken(2, s, start_pos);
        }
      }

      return new SqlToken(3, s, start_pos);
    }

    if (this.fChar == '-') {
      this.fPos += 1;
      char ch2 = this.fBefore.charAt(this.fPos);

      if (ch2 != '-') {
        return new SqlToken(1, "-", start_pos);
      }

      this.fPos += 1;
      String s = "--";
      do {
        this.fChar = this.fBefore.charAt(this.fPos);
        s = s + this.fChar;
        this.fPos += 1;
      }while ((this.fChar != '\n') && (this.fPos < this.fBefore.length()));
      return new SqlToken(5, s, start_pos);
    }

    if (this.fChar == '/') {
      this.fPos += 1;
      char ch2 = this.fBefore.charAt(this.fPos);

      if (ch2 != '*') {
        return new SqlToken(1, "/", start_pos);
      }

      String s = "/*";
      this.fPos += 1;
      int ch0 = -1;
      do {
        ch0 = this.fChar;
        this.fChar = this.fBefore.charAt(this.fPos);
        s = s + this.fChar;
        this.fPos += 1;
      }while ((ch0 != 42) || (this.fChar != '/'));
      return new SqlToken(5, s, start_pos);
    }

    if (this.fChar == '\'') {
      this.fPos += 1;
      String s = "'";
      do {
        this.fChar = this.fBefore.charAt(this.fPos);
        s = s + this.fChar;
        this.fPos += 1;
      }while (this.fChar != '\'');
      return new SqlToken(4, s, start_pos);
    }

    if (this.fChar == '"') {
      this.fPos += 1;
      String s = "\"";
      do {
        this.fChar = this.fBefore.charAt(this.fPos);
        s = s + this.fChar;
        this.fPos += 1;
      }while (this.fChar != '"');
      return new SqlToken(3, s, start_pos);
    }

    if (isSymbol(this.fChar))
    {
      String s = "" + this.fChar;
      this.fPos += 1;
      if (this.fPos >= this.fBefore.length()) {
        return new SqlToken(1, s, start_pos);
      }

      char ch2 = this.fBefore.charAt(this.fPos);
      for (int i = 0; i < twoCharacterSymbol.length; i++) {
        if ((twoCharacterSymbol[i].charAt(0) == this.fChar) && (twoCharacterSymbol[i].charAt(1) == ch2))
        {
          this.fPos += 1;
          s = s + ch2;
          break;
        }
      }
      return new SqlToken(1, s, start_pos);
    }

    this.fPos += 1;
    return new SqlToken(7, "" + this.fChar, start_pos);
  }

  public List<SqlToken> parse(String argSql)
  {
    this.fPos = 0;
    this.fBefore = argSql;

    List list = new ArrayList();
    while (true) {
      SqlToken token = nextToken();
      if (token.getType() == 6)
      {
        break;
      }
      list.add(token);
    }
    return list;
  }
}
