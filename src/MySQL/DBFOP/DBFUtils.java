package MySQL.DBFOP;

import java.io.*;
import java.util.*;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;
import MySQL.DBMS;

public class DBFUtils {

    public DBFUtils() {

    }

    //从DBF文件中获取数据
    public static DBFContent getFileData(String tableName) {
        DBFReader reader = null;// 从dbf中获取内容
        List<Map<String, Object>> list = new LinkedList<>();
        DBFContent content = new DBFContent();
        InputStream in = null;
        try {
            in = new FileInputStream(new File(DBMS.DATA_PATH + tableName
                    + ".dbf"));
            reader = new DBFReader(in);// 将文件从文件流中读入。

            int fcount = reader.getFieldCount();// 读取字段个数
            int rowNo = reader.getRecordCount();// 获取有多少条记录

            // 设置DBFContent的参数
            content.setFieldCount(fcount);
            content.setRecordCount(rowNo);

            List<DBFField> fields = new ArrayList<DBFField>();
            for (int j = 0; j < fcount; j++) {// dbf
                // 指定列的宽度大小
                reader.getField(j).setFieldLength(
                        reader.getField(j).getFieldLength());

                // 得到DBF文件的Fields
                fields.add(reader.getField(j));
            }
            content.setFields(fields);

            Object[] rowObjects;// 获取一个文件的行数
            List<Map<String, Object>> cons = new ArrayList<Map<String, Object>>();
            while ((rowObjects = reader.nextRecord()) != null) {
                Map<String, Object> map = new HashMap<String, Object>();// map为新dbf每一行的所有值数组。
                for (int i = 0; i < fcount; i++) {
                    String s = rowObjects[i].toString().trim();
                    //值为空处理
                    if (s == null || s.equals("") || s.length() <= 0
                            || s.isEmpty() || "null".equals(s)) {
                        map.put(fields.get(i).getName(), "");
                    } else {
                        map.put(fields.get(i).getName(), s);
                    }
                }
                cons.add(map);
            }

            content.setContents(cons);
        } catch (FileNotFoundException e) {
            System.err.println("In the process of parsing dbf data, the dbf file was not found");
            e.printStackTrace();
        } catch (DBFException e) {
            System.err.println("In the process of parsing dbf data, reading the dbf file is abnormal");
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return content;
    }

    // 创建空表
    public static void createDBF(String tableName,
                                 List<Map<String, Object>> list) {
        OutputStream fos = null;
        try {
            // 定义DBF文件字段
            DBFField[] fields = new DBFField[list.size()];
            // 分别定义各个字段信息
            for (int i = 0; i < list.size(); i++) {
                fields[i] = new DBFField();
                fields[i].setName((String) list.get(i).get("fieldName"));
                fields[i].setDataType((byte) list.get(i).get("type"));
                fields[i].setFieldLength((int) list.get(i).get("length"));
            }

            // 定义DBFWriter实例用来写DBF文件
            DBFWriter writer = new DBFWriter();
            // 把字段信息写入DBFWriter实例，即定义表结构
            writer.setFields(fields);
            // 定义输出流，并关联的一个文件
            fos = new FileOutputStream(DBMS.DATA_PATH + tableName + ".dbf");
            // 写入数据
            writer.write(fos);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (Exception e) {
            }
        }
    }

    // 创建表，添加记录内容
    public static void insertDBF(String tableName, DBFContent content) {
        OutputStream fos = null;
        try {
            // 定义DBF文件字段
            DBFField[] fields = new DBFField[content.getFields().size()];
            // 分别定义各个字段信息
            for (int i = 0; i < content.getFields().size(); i++) {
                fields[i] = new DBFField();
                fields[i].setName(content.getFields().get(i).getName());
                fields[i].setDataType(content.getFields().get(i).getDataType());
                fields[i].setFieldLength(content.getFields().get(i)
                        .getFieldLength());
            }

            // 定义DBFWriter实例用来写DBF文件
            DBFWriter writer = new DBFWriter();
            // 把字段信息写入DBFWriter实例，即定义表结构
            writer.setFields(fields);

            // 一条条的写入记录
            Object[] rowData = new Object[content.getFieldCount()];
            for (int i = 0; i < content.getContents().size(); i++) {
                rowData = new Object[content.getFieldCount()];
                Map<String, Object> record = content.getContents().get(i);
                for (int j = 0; j < content.getFields().size(); j++) {
                    String field = content.getFields().get(j).getName();
                    Object value = record.get(field);
                    if (content.getFields().get(j).getDataType() == DBFField.FIELD_TYPE_N)// Double数字类型特别处理
                        value = Double.parseDouble((String) record.get(field));
                    System.out.println("field = " + field + "   value = "
                            + value);
                    rowData[j] = value;
                }
                writer.addRecord(rowData);
            }

            // 定义输出流，并关联的一个文件
            File file = new File(DBMS.DATA_PATH + tableName + ".dbf");
            if (!file.exists())
                file.createNewFile();
            fos = new FileOutputStream(file);
            // 写入数据
            writer.write(fos);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (Exception e) {
            }
        }
    }

}
