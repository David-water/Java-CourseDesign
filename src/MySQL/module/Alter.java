package MySQL.module;

import java.io.*;
import java.util.*;
import com.linuxense.javadbf.DBFField;
import MySQL.DBMS;
import MySQL.MyException;
import MySQL.DBFOP.DBFContent;
import MySQL.DBFOP.DBFUtils;

public class Alter {

    public static final int ADD = 201;
    public static final int DROP = 202;

    private String tableName, columnName, dataType;// 表名，列名，列数据类型
    private int type;// 标记列数据类型
    private DBFContent content;// DBF文件内容

    public Alter(String tableName, String type, String other)
            throws MyException {
        this.tableName = tableName;
        File file = new File(DBMS.DATA_PATH + tableName + ".dbf");
        if (!file.exists()) {
            // 表不存在，抛出异常
            throw new MyException("Table does not exist");
        }
        content = DBFUtils.getFileData(tableName);
        switch (type) {
            case "add":
                this.type = ADD;
                String[] temp = other.split(" ");
                columnName = temp[0];
                dataType = temp[1];
                break;
            case "drop":
                this.type = DROP;
                String[] temp1 = other.split(" ");
                if (!"column".equals(temp1[0])) {
                    // column拼写错误，抛出异常
                    throw new MyException("column拼写错误");
                }
                columnName = temp1[1];
                break;
            default:
                break;
        }
    }

    public String excuteSQL() throws MyException {
        switch (type) {
            case ADD:
                addColumn();
                return "添加列" + columnName + "成功";
            case DROP:
                dropColumn();
                return "删除列" + columnName + "成功";
            default:
                break;
        }
        return null;
    }
    //删除列操作
    private void dropColumn() {
        DBFField needBeDelete = null;
        for (DBFField field : content.getFields()) {
            if (field.getName().equals(columnName))
                needBeDelete = field;
        }
        content.getFields().remove(needBeDelete);

        content.setFieldCount(content.getFieldCount() - 1);
        List<Map<String, Object>> list = content.getContents();
        for (Map<String, Object> map : list) {
            map.remove(columnName);
        }
        content.setContents(list);
        DBFUtils.insertDBF(tableName, content);
    }
    //添加列操作，所有记录中此列值为空，数字类型为0
    private void addColumn() throws MyException {
        DBFField field = new DBFField();
        field.setName(columnName);
        switch (dataType) {
            case "int":
                field.setDataType(DBFField.FIELD_TYPE_N);
                field.setFieldLength(10);
                break;
            case "double":
                field.setDataType(DBFField.FIELD_TYPE_N);
                field.setFieldLength(10);
                break;
            default:
                if (dataType.startsWith("char")) {
                    field.setDataType(DBFField.FIELD_TYPE_C);
                    if (!dataType.contains("(")) {
                        // 没有标明char长度，抛出异常
                        throw new MyException("没有标明char长度");
                    }
                    try {
                        int length = Integer.parseInt(dataType.substring(
                                dataType.indexOf("(") + 1, dataType.indexOf(")")));
                        field.setFieldLength(length);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        // 类型转换错误，抛出异常
                        throw new MyException("数字类型转换错误");
                    }
                } else {
                    // 输入的数据类型不对，抛出异常
                    throw new MyException("包含数据库系统不具有的数据类型");
                }
                break;
        }
        content.getFields().add(field);
        content.setFieldCount(content.getFieldCount() + 1);
        List<Map<String, Object>> list = content.getContents();
        for (Map<String, Object> map : list) {
            switch (dataType) {
                case "int":
                    map.put(columnName, "0");
                    break;
                case "double":
                    map.put(columnName, "0");
                    break;
                default:
                    if (dataType.startsWith("char")) {
                        map.put(columnName, "null");
                    }
                    break;
            }
        }
        content.setContents(list);
        DBFUtils.insertDBF(tableName, content);
    }
}

