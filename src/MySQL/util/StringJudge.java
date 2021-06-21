package MySQL.util;

public class StringJudge {
    public void StringTools() {
        // TODO 自动生成的构造函数存根
    }

    //判断字符串是否为数字
    public static boolean isInteger(String str) {
        try {
            int t = Integer.parseInt(str);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    //判断sql语句中的字符串是否为字段名
    public static boolean isFieldName(String str) {
        if (str.contains("'"))
            return false;
        else {
            try {
                double d = Double.parseDouble(str);
                return false;
            } catch (Exception e) {
                return true;
            }
        }
    }
}
