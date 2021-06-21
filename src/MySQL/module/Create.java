package MySQL.module;

import java.io.*;
import java.util.*;
import com.linuxense.javadbf.DBFField;
import MySQL.DBMS;
import MySQL.MyException;
import MySQL.DBFOP.DBFContent;
import MySQL.DBFOP.DBFUtils;

public class Create {

    private List<Map<String, Object>> fields;//字段
    private String tableName;//表名
    private boolean isConstraint = false;//是否有约束条件

    public Create(List<String[]> fields, String tableName) throws MyException {
        super();
        this.tableName = tableName;
        this.fields = new ArrayList<Map<String, Object>>();
        init(fields);
    }

    private void init(List<String[]> fields) throws MyException {
        for (String[] field : fields) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("fieldName", field[0]);
            byte type = 0;
            int length = 0;
            if (field[1].equals("int")) {
                type = DBFField.FIELD_TYPE_N;
                length = 10;
            } else if (field[1].equals("double")) {
                type = DBFField.FIELD_TYPE_N;
                length = 10;
            } else if (field[1].startsWith("char")) {
                type = DBFField.FIELD_TYPE_C;
                try {
                    String lString = field[1].substring(
                            field[1].indexOf("(") + 1, field[1].indexOf(")"));
                    length = Integer.parseInt(lString);
                } catch (Exception e) {
                    // 不是数字，char后面的参数错误，抛出异常
                    throw new MyException("char长度参数出错");
                }
            } else {
                // 数据类型错误，抛出异常
                throw new MyException("char长度参数为非数字");
            }
            map.put("length", length);
            map.put("type", type);
            map.put("constraint", 0);
            // 读取完整性约束条件
            if (field.length > 2) {
                isConstraint = true;
                // 数组个数超过2 ，表示有完整性约束
                int constraint = 0;
                for (int i = 2; i < field.length; i++) {
                    switch (field[i]) {
                        case "primary":
                            if ("key".equals(field[i + 1])) {
                                constraint += Constraint.PRIMARY_KEY;
                                i++;
                            } else {
                                // key拼写错误，抛出异常
                                throw new MyException("primary key中key拼写错误");
                            }
                            break;
                        case "not":
                            if ("null".equals(field[i + 1])) {
                                constraint += Constraint.NOT_NULL;
                                i++;
                            } else {
                                // null拼写错误，抛出异常
                                throw new MyException("not null中null拼写错误");
                            }
                            break;
                        case "unique":
                            constraint += Constraint.UNIQUE;
                            i++;
                            break;
                        default:
                            break;
                    }
                }
                map.put("constraint", constraint);
            }
            this.fields.add(map);
        }
    }

    public DBFContent exccuteSQL() throws MyException {
        // 读取类中的创建表的数据，在磁盘中创建表，表名为文件名
        File file = new File(DBMS.DATA_PATH + tableName + ".dbf");
        if (file.exists()) {
            // 表已存在，抛出异常
            throw new MyException("表已存在");
        }
        DBFUtils.createDBF(tableName, fields);
        if (isConstraint)
            inputContraint();
        DBFContent content = DBFUtils.getFileData(tableName);
        content.setTableName(tableName);
        System.out.println(content.getFields());
        return content;
    }

    //读取约束性条件表，数据字典
    private void inputContraint() {
        File file = new File(DBMS.DATA_PATH + "constraint.dbf");
        if (!file.exists()) {//文件不存在，创建，表中有tableName,fieldName,PrimaryKey,Unique,NotNull五个属性
            DBFContent content = new DBFContent();
            content.setFieldCount(5);
            content.setRecordCount(fields.size());
            System.out.println("field = " + fields);

            List<Map<String, Object>> list = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = (String) fields.get(i).get("fieldName");
                System.out.println(fields);
                int constraint = (Integer) fields.get(i).get("constraint");
                Map<String, Object> map = getConstraintRecord(fieldName,
                        constraint);
                System.out.println(" map init = " + map);
                list.add(map);
            }
            content.setContents(list);

            List<DBFField> dbfFields = new ArrayList<DBFField>();

            DBFField dbfField = new DBFField();
            dbfField.setName("fieldName");
            dbfField.setDataType(DBFField.FIELD_TYPE_C);
            dbfField.setFieldLength(10);
            dbfFields.add(dbfField);

            dbfField = new DBFField();
            dbfField.setName("tableName");
            dbfField.setDataType(DBFField.FIELD_TYPE_C);
            dbfField.setFieldLength(10);
            dbfFields.add(dbfField);

            dbfField = new DBFField();
            dbfField.setName("PrimaryKey");
            dbfField.setDataType(DBFField.FIELD_TYPE_C);
            dbfField.setFieldLength(10);
            dbfFields.add(dbfField);

            dbfField = new DBFField();
            dbfField.setName("Unique");
            dbfField.setDataType(DBFField.FIELD_TYPE_C);
            dbfField.setFieldLength(10);
            dbfFields.add(dbfField);

            dbfField = new DBFField();
            dbfField.setName("NotNull");
            dbfField.setDataType(DBFField.FIELD_TYPE_C);
            dbfField.setFieldLength(10);
            dbfFields.add(dbfField);

            content.setFields(dbfFields);

            DBFUtils.insertDBF("constraint", content);
        } else {
            //将fields中的约束性条件字符串填入到数据字典中
            DBFContent content = DBFUtils.getFileData("constraint");
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = (String) fields.get(i).get("fieldName");
                int constraint = (Integer) fields.get(i).get("constraint");
                Map<String, Object> map = getConstraintRecord(fieldName,
                        constraint);
                content.getContents().add(map);
                content.setRecordCount(content.getRecordCount() + 1);
            }
            DBFUtils.insertDBF("constraint", content);
        }
    }

    //获取完整性约束条件
    private Map<String, Object> getConstraintRecord(String fieldName,
                                                    int constraint) {
        switch (constraint) {
            case 1:
                Map<String, Object> map1 = new HashMap<>();
                map1.put("fieldName", fieldName);
                map1.put("tableName", tableName);
                map1.put("PrimaryKey", true + "");
                map1.put("Unique", false + "");
                map1.put("NotNull", false + "");
                System.out.println("map1 = " + map1);
                return map1;
            case 2:
                Map<String, Object> map2 = new HashMap<>();
                map2.put("fieldName", fieldName);
                map2.put("tableName", tableName);
                map2.put("PrimaryKey", false + "");
                map2.put("Unique", true + "");
                map2.put("NotNull", false + "");
                System.out.println("map2 = " + map2);
                return map2;
            case 3:
                Map<String, Object> map3 = new HashMap<>();
                map3.put("fieldName", fieldName);
                map3.put("tableName", tableName);
                map3.put("PrimaryKey", true + "");
                map3.put("Unique", true + "");
                map3.put("NotNull", false + "");
                System.out.println("map3 = " + map3);
                return map3;
            case 4:
                Map<String, Object> map4 = new HashMap<>();
                map4.put("fieldName", fieldName);
                map4.put("tableName", tableName);
                map4.put("PrimaryKey", false + "");
                map4.put("Unique", false + "");
                map4.put("NotNull", true + "");
                System.out.println("map4 = " + map4);
                return map4;
            case 5:
                Map<String, Object> map5 = new HashMap<>();
                map5.put("fieldName", fieldName);
                map5.put("tableName", tableName);
                map5.put("PrimaryKey", true + "");
                map5.put("Unique", false + "");
                map5.put("NotNull", true + "");
                System.out.println("map5 = " + map5);
                return map5;
            case 6:
                Map<String, Object> map6 = new HashMap<>();
                map6.put("fieldName", fieldName);
                map6.put("tableName", tableName);
                map6.put("PrimaryKey", false + "");
                map6.put("Unique", true + "");
                map6.put("NotNull", true + "");
                System.out.println("map6 = " + map6);
                return map6;
            case 7:
                Map<String, Object> map7 = new HashMap<>();
                map7.put("fieldName", fieldName);
                map7.put("tableName", tableName);
                map7.put("PrimaryKey", true + "");
                map7.put("Unique", true + "");
                map7.put("NotNull", true + "");
                System.out.println("map7 = " + map7);
                return map7;
            default:
                Map<String, Object> map0 = new HashMap<>();
                map0.put("fieldName", fieldName);
                map0.put("tableName", tableName);
                map0.put("PrimaryKey", false + "");
                map0.put("Unique", false + "");
                map0.put("NotNull", false + "");
                System.out.println("map1 = " + map0);
                return map0;
        }
    }

    public class Constraint {

        public static final int PRIMARY_KEY = 1;
        public static final int UNIQUE = 2;
        public static final int NOT_NULL = 4;
    }
}