package commons.sql.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Stack;

import commons.sql.format.valueobject.SqlToken;

public class SqlFormatter
{
  private final SqlParser fParser = new SqlParser();

  private SqlRule fRule = null;

  private Stack<Boolean> functionBracket = new Stack();

  public SqlFormatter(SqlRule argRule)
  {
    this.fRule = argRule;
  }

  public String format(String argSql)
    throws SqlFormatterException
  {
    this.functionBracket.clear();
    try {
      boolean isSqlEndsWithNewLine = false;
      if (argSql.endsWith("\n")) {
        isSqlEndsWithNewLine = true;
      }

      List list = this.fParser.parse(argSql);

      list = format(list);

      String after = "";
      for (int index = 0; index < list.size(); index++) {
        SqlToken token = (SqlToken)list.get(index);
        after = after + token.getString();
      }

      if (isSqlEndsWithNewLine);
      return after + "\n";
    }
    catch (Exception ex)
    {
      SqlFormatterException sqlException = new SqlFormatterException(ex.toString());

      sqlException.initCause(ex);
      throw sqlException;
    }
  }

  public List<SqlToken> format(List<SqlToken> argList)
  {
    SqlToken token = (SqlToken)argList.get(0);
    if (token.getType() == 0) {
      argList.remove(0);
    }

    token = (SqlToken)argList.get(argList.size() - 1);
    if (token.getType() == 0) {
      argList.remove(argList.size() - 1);
    }

    for (int index = 0; index < argList.size(); index++) {
      token = (SqlToken)argList.get(index);
      if (token.getType() == 2) {
        switch (this.fRule.keyword) {
        case 0:
          break;
        case 1:
          token.setString(token.getString().toUpperCase());
          break;
        case 2:
          token.setString(token.getString().toLowerCase());
        }

      }

    }

    for (int index = argList.size() - 1; index >= 1; index--) {
      token = (SqlToken)argList.get(index);
      SqlToken prevToken = (SqlToken)argList.get(index - 1);
      if ((token.getType() == 0) && ((prevToken.getType() == 1) || (prevToken.getType() == 5)))
      {
        argList.remove(index);
      } else if (((token.getType() == 1) || (token.getType() == 5)) && (prevToken.getType() == 0))
      {
        argList.remove(index - 1);
      } else if (token.getType() == 0) {
        token.setString(" ");
      }

    }

    for (int index = 0; index < argList.size() - 2; index++) {
      SqlToken t0 = (SqlToken)argList.get(index);
      SqlToken t1 = (SqlToken)argList.get(index + 1);
      SqlToken t2 = (SqlToken)argList.get(index + 2);

      if ((t0.getType() == 2) && (t1.getType() == 0) && (t2.getType() == 2))
      {
        if (((t0.getString().equalsIgnoreCase("ORDER")) || (t0.getString().equalsIgnoreCase("GROUP"))) && (t2.getString().equalsIgnoreCase("BY")))
        {
          t0.setString(t0.getString() + " " + t2.getString());
          argList.remove(index + 1);
          argList.remove(index + 1);
        }

      }

      if ((t0.getString().equals("(")) && (t1.getString().equals("+")) && (t2.getString().equals(")")))
      {
        t0.setString("(+)");
        argList.remove(index + 1);
        argList.remove(index + 1);
      }

    }

    int indent = 0;

    Stack bracketIndent = new Stack();
    SqlToken prev = new SqlToken(0, " ");

    boolean encounterBetween = false;
    boolean encounterConcate = false;
    for (int index = 0; index < argList.size(); index++) {
      token = (SqlToken)argList.get(index);
      if (token.getType() == 1)
      {
    	
    	  
        if (token.getString().equals("(")) {
          this.functionBracket.push(this.fRule.isFunction(prev.getString()) ? Boolean.TRUE : Boolean.FALSE);

          bracketIndent.push(new Integer(indent));
          indent++;
          if(!encounterConcate)  //exception for count(*) case and function case
        	  index += insertReturnAndIndent(argList, index + 1, indent);
          encounterConcate = false;
        }
        else if (token.getString().equals(")")) {
          indent = ((Integer)bracketIndent.pop()).intValue();
          index += insertReturnAndIndentCustomized(argList, index+1, indent);
          this.functionBracket.pop();
        }
        else if (token.getString().equals(",")) {
          index += insertReturnAndIndent(argList, index+1, indent);
        } else if (token.getString().equals(";"))
        {
          indent = 0;
          index += insertReturnAndIndent(argList, index+1, indent);
        }
      } else if (token.getType() == 2)
      {
    	  if (token.getString().equalsIgnoreCase("COUNT")||token.getString().equalsIgnoreCase("FUNCTION")||
      			token.getString().equalsIgnoreCase("MAX")) {
                encounterConcate = true;
               
              }    
    	  if((token.getString().equalsIgnoreCase("FUNCTION")) )  
      	{
      	  index += insertReturnAndIndent(argList, index , 0);
      	}  
    	  
//    	  if((token.getString().equalsIgnoreCase("IS")) )  
//        	{
//    		  indent++;
//        	  index += insertReturnAndIndent(argList, index+1 , indent);
//        	}  
//      	  
    	  
    	  
    	if((token.getString().equalsIgnoreCase("BEGIN")) )  
    	{
    	 indent += 1;	
    	  index += insertReturnAndIndent(argList, index + 1, indent);
    	}
    	  
    	  
        if ((token.getString().equalsIgnoreCase("DELETE")) || (token.getString().equalsIgnoreCase("SELECT")))
        {
          indent += 2;          
          if(! argList.get(index).getString().startsWith(";"))
          {
        	  index += insertReturnAndIndent(argList, index, indent-2);
          }
          index += insertReturnAndIndent(argList, index + 1, indent);
        }
        
        
        if ((token.getString().equalsIgnoreCase("UPDATE"))){
        	 indent++;
        }

        
        if ((token.getString().equalsIgnoreCase("INSERT")) )
        {
          indent++;
        }
        
        
        if ((token.getString().equalsIgnoreCase("CREATE")) || (token.getString().equalsIgnoreCase("DROP")) || (token.getString().equalsIgnoreCase("TRUNCATE")) || (token.getString().equalsIgnoreCase("TABLE")) || (token.getString().equalsIgnoreCase("CASE")))
        {
          indent++;
          index += insertReturnAndIndent(argList, index + 1, indent);
        }
        
        if ( (token.getString().equalsIgnoreCase("INTO")) )  //Make into one indent
        {
//          indent++;
          index += insertReturnAndIndent(argList, index, indent-1);
        }
        
        if( (token.getString().equalsIgnoreCase("SET"))){
        	indent++;
        	 index += insertReturnAndIndent(argList, index + 1, indent);
        }

        if ((token.getString().equalsIgnoreCase("FROM"))  || (token.getString().equalsIgnoreCase("ORDER BY")) || (token.getString().equalsIgnoreCase("GROUP BY")) || (token.getString().equalsIgnoreCase("HAVING")))
        {        	
          index += insertReturnAndIndent(argList, index, indent - 1); //Indent means one tab
          index += insertReturnAndIndent(argList, index + 1, indent);
        }
        
        if ((token.getString().equalsIgnoreCase("WHERE")) )
        {
        	 index += insertReturnAndIndent(argList, index, indent - 1); //Indent means one tab
             index += insertReturnAndIndent(argList, index + 1, indent);
        }

        if (token.getString().equalsIgnoreCase("VALUES")) {
//          indent++;
          index += insertReturnAndIndent(argList, index, indent);
        }

        if (token.getString().equalsIgnoreCase("END")) {
          indent--;
          index += insertReturnAndIndent(argList, index, indent);
        }

        if ((token.getString().equalsIgnoreCase("OR")) || (token.getString().equalsIgnoreCase("THEN")) || (token.getString().equalsIgnoreCase("ELSE")))
        {
          index += insertReturnAndIndent(argList, index, indent);
        }

        if ((token.getString().equalsIgnoreCase("ON")) || (token.getString().equalsIgnoreCase("USING")))
        {
          index += insertReturnAndIndent(argList, index, indent + 1);
        }

        if ((token.getString().equalsIgnoreCase("UNION")) || (token.getString().equalsIgnoreCase("INTERSECT")) || (token.getString().equalsIgnoreCase("EXCEPT")))
        {
          indent -= 2;
          index += insertReturnAndIndent(argList, index, indent);
          index += insertReturnAndIndent(argList, index + 1, indent);
        }
        if (token.getString().equalsIgnoreCase("BETWEEN")) {
          encounterBetween = true;
        }
        if (token.getString().equalsIgnoreCase("AND"))
        {
          if (!encounterBetween) {
            index += insertReturnAndIndent(argList, index, indent-1);
          }

          encounterBetween = false;
        }
      } else if ((token.getType() == 5) && 
        (token.getString().startsWith("/*")))
      {
        index += insertReturnAndIndent(argList, index + 1, indent);
      }

      prev = token;
    }

    for (int index = argList.size() - 1; index >= 4; index--) {
      if (index < argList.size())
      {
        SqlToken t0 = (SqlToken)argList.get(index);
        SqlToken t1 = (SqlToken)argList.get(index - 1);
        SqlToken t2 = (SqlToken)argList.get(index - 2);
        SqlToken t3 = (SqlToken)argList.get(index - 3);
        SqlToken t4 = (SqlToken)argList.get(index - 4);

        if ((t4.getString().equalsIgnoreCase("(")) && (t3.getString().trim().equalsIgnoreCase("")) && (t1.getString().trim().equalsIgnoreCase("")) && (t0.getString().equalsIgnoreCase(")")))
        {
          t4.setString(t4.getString() + t2.getString() + t0.getString());
          argList.remove(index);
          argList.remove(index - 1);
          argList.remove(index - 2);
          argList.remove(index - 3);
        }
      }
    }

    for (int index = 1; index < argList.size(); index++) {
      prev = (SqlToken)argList.get(index - 1);
      token = (SqlToken)argList.get(index);

      if ((prev.getType() != 0) && (token.getType() != 0))
      {
        if (!prev.getString().equals(","))
        {
          if ((!this.fRule.isFunction(prev.getString())) || (!token.getString().equals("(")))
          {
            argList.add(index, new SqlToken(0, " "));
          }
        }
      }
    }
    return argList;
  }

  private int insertReturnAndIndent(List<SqlToken> argList, int argIndex, int argIndent)
  {
    if (this.functionBracket.contains(Boolean.TRUE))
      return 0;
    try
    {
      String s = "\n";

      if(argIndex>=1)
      {
      SqlToken prevToken = (SqlToken)argList.get(argIndex - 1);
      if ((prevToken.getType() == 5) && (prevToken.getString().startsWith("--")))
      {
        s = "";
      }
      }
      
      
      /*To set insert into in the same line*/
      if(argIndex>=2)
      {
      SqlToken prevprevToken = (SqlToken)argList.get(argIndex - 2);
      if ((prevprevToken.getString().startsWith("INSERT")))
      {
        s = " ";
        argIndent--;
      }
      }

      for (int index = 0; index < argIndent; index++) {
        s = s + this.fRule.indentString;
      }
      

      SqlToken token = (SqlToken)argList.get(argIndex);
      if (token.getType() == 0) {
        token.setString(s);
        return 0;
      }

      token = (SqlToken)argList.get(argIndex - 1);
      if (token.getType() == 0) {
        token.setString(s);
        return 0;
      }

      argList.add(argIndex, new SqlToken(0, s));

      return 1;
    } catch (IndexOutOfBoundsException e) {
    	//omit exception
//    	e.printStackTrace();  
    }
    return 0;
  }
  
  
  
  
  
  
  private int insertReturnAndIndentCustomized(List<SqlToken> argList, int argIndex, int argIndent)
  {

	    if (this.functionBracket.contains(Boolean.TRUE))
	        return 0;
	      try
	      {
	        String s = "";
	        SqlToken prevToken = (SqlToken)argList.get(argIndex - 1);
	        if ((prevToken.getType() == 5) && (prevToken.getString().startsWith("--")))
	        {
	          s = "";
	        }

	        
	       s = s+ " ";

	        SqlToken token = (SqlToken)argList.get(argIndex);
	        if (token.getType() == 0) {
	          token.setString(s);
	          return 0;
	        }

	        token = (SqlToken)argList.get(argIndex - 1);
	        if (token.getType() == 0) {
	          token.setString(s);
	          return 0;
	        }

	        argList.add(argIndex, new SqlToken(0, s));

	        return 1;
	      } catch (IndexOutOfBoundsException e) {
	      }
	      return 0;
	    }

  
  
  
  
  
  
  
  public static void main(String[] args)
    throws Exception
  {
    SqlRule rule = new SqlRule();
    rule.keyword = 1;
    rule.indentString = "    ";
    String[] mySqlFuncs = { "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "BIT_COUNT", "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MAX", "MIN", "MOD", "PI", "POW", "POWER", "RADIANS", "RAND", "ROUND", "SIN", "SQRT", "TAN", "TRUNCATE", "ASCII", "BIN", "BIT_LENGTH", "CHAR", "CHARACTER_LENGTH", "CHAR_LENGTH", "CONCAT", "CONCAT_WS", "CONV", "ELT", "EXPORT_SET", "FIELD", "FIND_IN_SET", "HEX,INSERT", "INSTR", "LCASE", "LEFT", "LENGTH", "LOAD_FILE", "LOCATE", "LOCATE", "LOWER", "LPAD", "LTRIM", "MAKE_SET", "MATCH", "MID", "OCT", "OCTET_LENGTH", "ORD", "POSITION", "QUOTE", "REPEAT", "REPLACE", "REVERSE", "RIGHT", "RPAD", "RTRIM", "SOUNDEX", "SPACE", "STRCMP", "SUBSTRING", "SUBSTRING", "SUBSTRING", "SUBSTRING", "SUBSTRING_INDEX", "TRIM", "UCASE", "UPPER", "DATABASE", "USER", "SYSTEM_USER", "SESSION_USER", "PASSWORD", "ENCRYPT", "LAST_INSERT_ID", "VERSION", "DAYOFWEEK", "WEEKDAY", "DAYOFMONTH", "DAYOFYEAR", "MONTH", "DAYNAME", "MONTHNAME", "QUARTER", "WEEK", "YEAR", "HOUR", "MINUTE", "SECOND", "PERIOD_ADD", "PERIOD_DIFF", "TO_DAYS", "FROM_DAYS", "DATE_FORMAT", "TIME_FORMAT", "CURDATE", "CURRENT_DATE", "CURTIME", "CURRENT_TIME", "NOW", "SYSDATE", "CURRENT_TIMESTAMP", "UNIX_TIMESTAMP", "FROM_UNIXTIME", "SEC_TO_TIME", "TIME_TO_SEC" };

    rule.setFunctionNames(mySqlFuncs);
    SqlFormatter formatter = new SqlFormatter(rule);

    File[] files = new File("Test").listFiles();
    for (int i = 0; i < files.length; i++) {
      System.out.println("-- " + files[i]);

      BufferedReader reader = new BufferedReader(new FileReader(files[i]));

      String before = "";
      while (reader.ready()) {
        String line = reader.readLine();
        if (line == null)
          break;
        before = before + line + "\n";
      }
      reader.close();

      System.out.println("[before]\n" + before);
      String after = formatter.format(before);
      System.out.println("[after]\n" + after);
    }
  }
}
