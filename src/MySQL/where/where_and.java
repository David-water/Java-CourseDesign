package MySQL.where;

import java.util.*;
import com.linuxense.javadbf.DBFField;
import MySQL.MyException;

public class where_and {
    public static final int BETWEEN = 101;
    public static final int EQUAL = 102;
    public static final int NOT_EQUAL = 103;
    public static final int BIGGER = 104;
    public static final int SMALLER = 105;
    public static final int SMALLER_EQUAL = 106;
    public static final int BIGGER_EQUAL = 107;

    public static final int FLAGS_VALUE = 201;
    public static final int FLAGS_FIELD = 202;

    private String table;
    private int type;// 条件类型
    private int count;// 参数数目
    private int[] flags;// 参数标志
    private String[] fields;// 参数或者参数名
    private String other;// 存储额外数据，between中的列名
    private List<DBFField> fieldsOfTable;//表中的字段

    public where_and(String table, List<DBFField> fieldsOfTable) {
        flags = new int[10];
        fields = new String[10];
        this.table = table;
        this.fieldsOfTable = fieldsOfTable;
    }

    //判断是否复合Where语句中的一个OR块条件
    public boolean judgeAnd(Map<String, Object> record) throws MyException {//
        switch (type) {
            case BETWEEN:
                try {
                    double value1 = 0, data = 0, value2 = 0;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = Double.parseDouble(fields[0]);
                    } else {
                        value1 = Double.parseDouble((String) record.get(fields[0]));
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = Double.parseDouble(fields[1]);
                    } else {
                        value2 = Double.parseDouble((String) record.get(fields[1]));
                    }
                    data = Double.parseDouble((String) record.get(other));
                    if ((data >= value1 && data <= value2)
                            || (data >= value2 && data <= value1)) {
                        return true;
                    } else
                        return false;
                } catch (Exception exception) {
                    // 转换出错，抛出异常，数据不是数字
                    throw new MyException("Between...AND...参数中还有非数字");
                }
            case EQUAL:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    try {
                        double v1 = Double.parseDouble(value1);
                        double v2 = Double.parseDouble(value2);
                        if ((v1 - v2) > -1e-6 && (v1 - v2) < 1e-6)
                            return true;
                        else
                            return false;
                    } catch (Exception e) {
                        // 转换出错，不是数字，用字符串比较
                        if (value1.equals(value2))
                            return true;
                        else
                            return false;
                    }
                } catch (Exception exception) {
                    // 转换出错，抛出异常
                    throw new MyException("数据类型转换错误");
                }
            case NOT_EQUAL:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    try {
                        double v1 = Double.parseDouble(value1);
                        double v2 = Double.parseDouble(value2);
                        if ((v1 - v2) > -1e-6 && (v1 - v2) < 1e-6)
                            return false;
                        else
                            return true;
                    } catch (Exception e) {
                        // 转换出错，不是数字，用字符串比较
                        if (value1.equals(value2))
                            return false;
                        else
                            return true;
                    }
                } catch (Exception exception) {
                    // 转换出错，抛出异常
                    throw new MyException("数据类型转换出错");
                }
            case BIGGER:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    try {
                        double v1 = Double.parseDouble(value1);
                        double v2 = Double.parseDouble(value2);
                        if (v1 > v2)
                            return true;
                        else
                            return false;
                    } catch (Exception e) {
                        // 转换出错，不是数字
                        throw new MyException(">比较参数中带有非数字");
                    }
                } catch (Exception exception) {
                    // 转换出错，抛出异常,不是数字
                    throw new MyException(">比较参数中带有非数字");
                }
            case BIGGER_EQUAL:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    double v1 = Double.parseDouble(value1);
                    double v2 = Double.parseDouble(value2);
                    if (((v1 - v2) > -1e-6 && (v1 - v2) < 1e-6) || v1 > v2)
                        return true;
                    else
                        return false;
                } catch (Exception exception) {
                    // 转换出错，抛出异常,不是数字
                    throw new MyException(">=比较参数中带有非数字");
                }
            case SMALLER:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    double v1 = Double.parseDouble(value1);
                    double v2 = Double.parseDouble(value2);
                    if (v1 < v2)
                        return true;
                    else
                        return false;
                } catch (Exception exception) {
                    // 转换出错，抛出异常,不是数字
                    throw new MyException("<比较参数中带有非数字");
                }
            case SMALLER_EQUAL:
                try {
                    String value1 = null, value2 = null;
                    if (flags[0] == FLAGS_VALUE) {
                        value1 = fields[0];
                    } else {
                        value1 = ((String) record.get(fields[0])).toLowerCase();
                    }
                    if (flags[1] == FLAGS_VALUE) {
                        value2 = fields[1];
                    } else {
                        value2 = ((String) record.get(fields[1])).toLowerCase();
                    }
                    double v1 = Double.parseDouble(value1);
                    double v2 = Double.parseDouble(value2);
                    if (((v1 - v2) > -1e-6 && (v1 - v2) < 1e-6) || v1 < v2)
                        return true;
                    else
                        return false;
                } catch (Exception exception) {
                    // 转换出错，抛出异常,不是数字
                    throw new MyException("<=比较参数中带有非数字");
                }
            default:
                break;
        }
        return false;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int[] getFlags() {
        return flags;
    }

    public void setFlags(int[] flags) {
        this.flags = flags;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public static int getAndType(String type2) {
        switch (type2) {
            case "<>":
                return NOT_EQUAL;
            case ">":
                return BIGGER;
            case "<":
                return SMALLER;
            case "=":
                return EQUAL;
            case ">=":
                return BIGGER_EQUAL;
            case "<=":
                return SMALLER_EQUAL;
            default:
                break;
        }
        return -1;
    }
}
