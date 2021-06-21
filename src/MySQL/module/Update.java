package MySQL.module;

import java.io.*;
import java.util.*;
import MySQL.DBMS;
import MySQL.MyException;
import MySQL.DBFOP.DBFContent;
import MySQL.DBFOP.DBFUtils;
import MySQL.util.StringJudge;
import MySQL.where.where_and;
import MySQL.where.where_or;

public class Update {
    private String tableName;//表名
    private List<Map<String, Object>> updates;//Update要更新的键值对，每个map为一个set赋值语句中的 键值
    public List<String> wheres;//Where条件字符串
    private List<where_or> conditions;//OR条件组
    private DBFContent content;//读取的 数据库文件内容

    public DBFContent excuteSQL() throws MyException {// 执行语句
        // 进行更新操作
        DBFContent resultContent = new DBFContent();
        List<Map<String, Object>> listNeedUpdate = new ArrayList<Map<String, Object>>();
        List<Map<String, Object>> temp = new ArrayList<Map<String, Object>>();
        for (int j = 0; j < content.getRecordCount(); j++) {
            int flag = 0;
            for (int k = 0; k < conditions.size(); k++) {
                System.out.println("j = " + j);
                if (conditions.get(k).judgeCondition(
                        content.getContents().get(j)) == false) {
                    continue;
                } else {
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) {
                System.out.println(content.getContents().get(j));
                listNeedUpdate.add(content.getContents().get(j));
            } else {
                System.out.println(content.getContents().get(j));
                temp.add(content.getContents().get(j));
            }
        }
        System.out.println(content);
        judgeConstraint(temp);// 判断约束条件
        listNeedUpdate = update(listNeedUpdate);// 将数据更新到列表
        for (int i = 0; i < listNeedUpdate.size(); i++) {
            temp.add(listNeedUpdate.get(i));
        }
        resultContent.setContents(temp);
        resultContent.setFieldCount(content.getFieldCount());
        resultContent.setFields(content.getFields());
        resultContent.setRecordCount(content.getRecordCount());
        DBFUtils.insertDBF(tableName, resultContent);
        return resultContent;
    }

    //将需要更新的键值对填充覆盖原来的值
    private List<Map<String, Object>> update(
            List<Map<String, Object>> listNeedUpdate) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> map : listNeedUpdate) {
            for (Map<String, Object> update : updates) {
                map.put((String) update.get("name"), update.get("value"));
            }
            result.add(map);
        }
        System.out.println("result update -->>" + result);
        return result;
    }
    //判断修改操作是否违背约束条件
    private void judgeConstraint(List<Map<String, Object>> temp) throws MyException {
        DBFContent constaintContent = DBFUtils.getFileData("constraint");
        for (Map<String, Object> constraint : constaintContent.getContents()) {
            System.out.println("--->>" + constraint.get("tableName"));
            if (((String) constraint.get("tableName")).equals(tableName)) {
                if (constraint.get("PrimaryKey").equals("true")) {
                    // 主键
                    String value = null;
                    boolean flag = false;
                    for (Map<String, Object> update : updates) {
                        if (update.get("name").equals(
                                constraint.get("fieldName"))) {
                            value = (String) update.get("value");
                            flag = true;
                        }
                    }
                    for (Map<String, Object> recordInFile : temp) {
                        if (((String) recordInFile.get(constraint
                                .get("fieldName"))).equals(value)) {
                            // 主键重复，抛出异常
                            throw new MyException("该修改操作违背该表的主键约束，主键重复");
                        }
                    }
                    if ((value == null || value.equals("null")) && flag) {
                        // 主键为空，抛出异常
                        throw new MyException("该修改操作违背该表的主键约束，主键为空");
                    }
                }
                if (constraint.get("Unique").equals("true")) {
                    // 唯一约束
                    String value = null;
                    for (Map<String, Object> recordInFile : temp) {
                        for (Map<String, Object> update : updates) {
                            if (update.get("name").equals(
                                    constraint.get("fieldName")))
                                value = (String) update.get("value");
                        }
                        if (((String) recordInFile.get(constraint
                                .get("fieldName"))).equals(value)) {
                            // 违背唯一约束，抛出异常
                            throw new MyException("该修改操作违背该表的唯一约束");
                        }
                    }
                }
                // 非空约束
                if (constraint.get("NotNull").equals("true")) {
                    String value = null;
                    boolean flag = false;
                    for (Map<String, Object> update : updates) {
                        if (update.get("name").equals(
                                constraint.get("fieldName"))) {
                            value = (String) update.get("value");
                            flag = true;
                        }
                    }
                    if ((value == null || value.equals("null")) && flag) {
                        // 违背非空约束，抛出异常
                        throw new MyException("该修改操作违背该表的非空约束");
                    }
                }
            }
        }
    }

    public Update(String tableName, String updateString, String whereString) throws MyException {
        this.tableName = tableName;
        File file = new File(DBMS.DATA_PATH + tableName + ".dbf");
        if (!file.exists()) {
            // 表不存在，抛出异常
            throw new MyException("表不存在");
        }
        content = DBFUtils.getFileData(tableName);
        wheres = new ArrayList<String>();
        updates = new ArrayList<Map<String, Object>>();
        for (String update : updateString.trim().split(",")) {
            String[] temp = update.split("=");
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("name", temp[0]);
            String valueTemp = temp[1].replace("'"," ").trim();//去掉引号
            map.put("value", valueTemp);
            updates.add(map);
        }
        for (String where : whereString.split(" ")) {
            wheres.add(where);
        }
        translateWhere();
    }

    private void translateWhere() throws MyException {// 翻译Where字符串为OR条件组
        conditions = new ArrayList<where_or>();
        where_or or = new where_or(tableName);
        for (int i = 0; i < wheres.size(); i++) {
            String w1 = wheres.get(i);
            switch (w1) {
                case "between":
                    where_and and1 = new where_and(tableName, content.getFields());
                    and1.setType(where_and.BETWEEN);
                    and1.setCount(2);
                    and1.setOther(wheres.get(i - 1));
                    int[] flags = new int[2];
                    String[] fields = new String[2];

                    if (StringJudge.isFieldName(wheres.get(i + 1))) {
                        flags[0] = where_and.FLAGS_FIELD;
                        fields[0] = wheres.get(i + 1);
                    } else {
                        flags[0] = where_and.FLAGS_VALUE;
                        String temp = wheres.get(i + 1);
                        if (temp.contains("'"))
                            temp = temp.replace("'", "");
                        if (temp.contains("'"))
                            temp = temp.replace("'", "");
                        fields[0] = temp;
                        System.out.println("temp  =  " + temp);
                    }

                    if (!wheres.get(i + 2).equals("and")) {
                        // and输入错误 抛出异常
                        throw new MyException("and拼写错误");
                    }

                    if (StringJudge.isFieldName(wheres.get(i + 3))) {
                        flags[1] = where_and.FLAGS_FIELD;
                        fields[1] = wheres.get(i + 3);
                    } else {
                        flags[1] = where_and.FLAGS_VALUE;
                        String temp = wheres.get(i + 3);
                        if (temp.contains("'"))
                            temp = temp.replace("'", "");
                        if (temp.contains("'"))
                            temp = temp.replace("'", "");
                        fields[1] = temp;
                        System.out.println("temp  =  " + temp);
                    }
                    and1.setFlags(flags);
                    and1.setFields(fields);
                    or.addAnd(and1);
                    i += 3;
                    break;
                case "and":

                    break;
                case "or":
                    conditions.add(or);
                    or = new where_or(tableName);
                    break;
                default:
                    where_and andOfWhere = new where_and(tableName,
                            content.getFields());
                    if (w1.contains("<>")) {
                        singleAndTranslate(or, w1, andOfWhere, "<>");
                    } else if (w1.contains("<=")) {
                        singleAndTranslate(or, w1, andOfWhere, "<=");
                    } else if (w1.contains(">=")) {
                        singleAndTranslate(or, w1, andOfWhere, ">=");
                    } else if (w1.contains("<")) {
                        singleAndTranslate(or, w1, andOfWhere, "<");
                    } else if (w1.contains(">")) {
                        singleAndTranslate(or, w1, andOfWhere, ">");
                    } else if (w1.contains("=")) {
                        singleAndTranslate(or, w1, andOfWhere, "=");
                    } else {
                        // 语句错误，抛出异常
                        throw new MyException("含有该数据库不支持的条件语句");
                    }
                    break;
            }
        }
        conditions.add(or);
    }

    public void singleAndTranslate(where_or or, String w1,
                                   where_and andOfWhere, String type) throws MyException {
        String[] temp = w1.split(type);
        if (temp.length != 2) {
            // 参数数目不正确，抛出异常
            System.out.println(" 参数数目不正确，抛出异常");
            throw new MyException("条件语句中参数数目不正确");
        } else {
            int[] flags1 = new int[2];
            String[] fields1 = new String[2];
            for (int j = 0; j < 2; j++) {
                if (StringJudge.isFieldName(temp[j])) {
                    flags1[j] = andOfWhere.FLAGS_FIELD;
                    fields1[j] = temp[j];
                    System.out.println("temp " + temp[j]);
                } else {
                    flags1[j] = andOfWhere.FLAGS_VALUE;
                    if (temp[j].contains("'"))
                        temp[j] = temp[j].replace("'", "");
                    if (temp[j].contains("'"))
                        temp[j] = temp[j].replace("'", "");
                    fields1[j] = temp[j];
                    System.out.println("temp " + temp[j]);
                }
            }
            andOfWhere.setType(andOfWhere.getAndType(type));
            andOfWhere.setCount(2);
            andOfWhere.setFields(fields1);
            andOfWhere.setFlags(flags1);
            or.addAnd(andOfWhere);
        }
    }

    /*
     * （非 Javadoc）
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Update [tableName=" + tableName + ", updates=" + updates
                + ", wheres=" + wheres + ", conditions=" + conditions
                + ", content=" + content + "]";
    }

}
