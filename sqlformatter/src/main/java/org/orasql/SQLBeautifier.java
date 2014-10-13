package org.orasql;

import blanco.commons.sql.format.SqlFormatter;
import blanco.commons.sql.format.SqlFormatterException;
import blanco.commons.sql.format.SqlRule;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class SQLBeautifier
{
  static final String[] functionsList = { "EMPTY_BLOB", "EMPTY_CLOB", "CONCAT", "INITCAP", "LOWER", "LPAD", "LTRIM", "NCHR", "NLS_INITCAP", "NLS_LOWER", "NLS_UPPER", "NLSSORT", "REGEXP_REPLACE", "REGEXP_SUBSTR", "REPLACE", "RPAD", "RTRIM", "SOUNDEX", "SUBSTR", "TRANSLATE", "TRIM", "UPPER", "INSTR", "LENGTH", "REGEXP_COUNT", "REGEXP_INSTR", "LNNVL", "NANVL", "NULLIF", "NVL", "NVL2", "CLUSTER_PROBABILITY", "CLUSTER_SET", "FEATURE_ID", "FEATURE_SET", "FEATURE_VALUE", "PREDICTION", "PREDICTION_BOUNDS", "PREDICTION_COST", "PREDICTION_DETAILS", "PREDICTION_PROBABILITY", "PREDICTION_SET", "NLS_CHARSET_ID", "NLS_CHARSET_NAME", "DUMP", "ORA_HASH", "VSIZE", "SYS_CONNECT_BY_PATH", "CURRENT_DATE", "CURRENT_TIMESTAMP", "DBTIMEZONE", "EXTRACT", "FROM_TZ", "LAST_DAY", "LOCALTIMESTAMP", "MONTHS_BETWEEN", "NEW_TIME", "NEXT_DAY", "NUMTODSINTERVAL", "NUMTOYMINTERVAL", "ORA_DST_AFFECTED", "ORA_DST_CONVERT", "ORA_DST_ERROR", "ROUND", "SESSIONTIMEZONE", "SYS_EXTRACT_UTC", "SYSDATE", "SYSTIMESTAMP", "TO_CHAR", "TO_DSINTERVAL", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ", "TO_YMINTERVAL", "TRUNC", "TZ_OFFSET", "COLLECT", "POWERMULTISET", "POWERMULTISET_BY_CARDINALITY", "SET", "SYS_GUID", "SYS_TYPEID", "UID", "USER", "USERENV", "LEAST", "BIN_TO_NUM", "CAST", "CHARTOROWID", "COMPOSE", "CONVERT", "DECOMPOSE", "HEXTORAW", "NUMTODSINTERVAL", "NUMTOYMINTERVAL", "RAWTOHEX", "RAWTONHEX", "ROWIDTOCHAR", "ROWIDTONCHAR", "SCN_TO_TIMESTAMP", "TIMESTAMP_TO_SCN", "TO_BINARY_DOUBLE", "TO_BINARY_FLOAT", "TO_BLOB", "TO_CHAR", "TO_CHAR", "TO_CHAR", "TO_CLOB", "TO_DATE", "TO_DSINTERVAL", "TO_LOB", "TO_MULTI_BYTE", "TO_NCHAR", "TO_NCHAR", "TO_NCHAR", "TO_NCLOB", "TO_NUMBER", "TO_SINGLE_BYTE", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ", "TO_YMINTERVAL", "TREAT", "UNISTR", "ACOS", "ASIN", "ATAN", "ATAN2", "BITAND", "CEIL", "COS", "COSH", "EXP", "FLOOR", "LN", "LOG", "MOD", "NANVL", "POWER", "REMAINDER", "ROUND", "SIGN", "SIN", "SINH", "SQRT", "TAN", "TANH", "TRUNC", "WIDTH_BUCKET", "DELETEXML", "DEPTH", "EXISTSNODE", "EXTRACT", "EXTRACTVALUE", "INSERTCHILDXML", "INSERTCHILDXMLAFTER", "INSERTCHILDXMLBEFORE", "INSERTXMLAFTER", "INSERTXMLBEFORE", "PATH", "SYS_DBURIGEN", "SYS_XMLAGG", "SYS_XMLGEN", "UPDATEXML", "XMLAGG", "XMLCAST", "XMLCDATA", "XMLCOLATTVAL", "XMLCOMMENT", "XMLCONCAT", "XMLDIFF", "XMLELEMENT", "XMLEXISTS", "XMLFOREST", "XMLISVALID", "XMLPARSE", "XMLPATCH", "XMLPI", "XMLQUERY", "XMLROOT", "XMLSEQUENCE", "XMLSERIALIZE", "XMLTABLE", "XMLTRANSFORM" };

  public static String beautify(String sqlText)
  {
    String formatedResult = "";
    SqlRule rule = new SqlRule();
    rule.setFunctionNames(functionsList);
    SqlFormatter formatter = new SqlFormatter(rule);
    try
    {
      formatedResult = formatter.format(sqlText);
    } catch (SqlFormatterException e) {
      formatedResult = "Error on parsing sql_text 1 level! Returning original sql: " + System.getProperty("line.separator") + sqlText;
    }

    return formatedResult;
  }

  public static String getInputSQL(String[] args) throws IOException
  {
    BufferedReader br = null;
    if (args.length > 0)
      br = new BufferedReader(new FileReader(args[0]));
    else {
      br = new BufferedReader(new InputStreamReader(System.in));
    }
    String sqlText = "";
    String x;
    while ((x = br.readLine()) != null) {
      sqlText = sqlText + x + System.getProperty("line.separator");
    }
    return sqlText;
  }

  public static void main(String[] args) {
    String inputString = null;
    try {
      inputString = getInputSQL(args);
    } catch (IOException e) {
      System.err.println("Cannot read input SQL!");
      e.printStackTrace();
    }
    System.out.print(beautify(inputString));
  }
}

/* Location:           E:\SQLBeautifier\
 * Qualified Name:     org.orasql.SQLBeautifier
 * JD-Core Version:    0.6.2
 */