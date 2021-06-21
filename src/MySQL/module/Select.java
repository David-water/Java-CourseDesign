package MySQL.module;
import java.io.*;
import java.util.*;
import com.linuxense.javadbf.DBFField;
import MySQL.DBMS;
import MySQL.MyException;
import MySQL.DBFOP.DBFContent;
import MySQL.DBFOP.DBFUtils;
import MySQL.util.StringJudge;
import MySQL.where.where_and;
import MySQL.where.where_or;

public class Select {
    private String[] originalSql;// 源sql字符串
    private int fromIndex, whereIndex, groupIndex, orderIndex;// 各个标志单词在sql分解成的字符串数组中的下标位置
    public List<Args> args;// 要查询的字段名称
    public List<String> froms, wheres, groups, orders;// Select语句中的各个语句块字符串
    private List<where_or> conditions;// 由Where语句生成的Or条件组
    private DBFContent content;// 文件读取的数据库内容

    @Override
    public String toString() {
        return "SelectSql [originalSql=" + Arrays.toString(originalSql)
                + ", fromIndex=" + fromIndex + ", whereIndex=" + whereIndex
                + ", groupIndex=" + groupIndex + ", orderIndex=" + orderIndex
                + ", args=" + args + ", froms=" + froms + ", wheres=" + wheres
                + ", groups=" + groups + ", orders=" + orders + "]";
    }

    public Select(String[] selects) throws MyException {
        originalSql = selects;
        fromIndex = whereIndex = groupIndex = orderIndex = -1;
        content = new DBFContent();
        for (int i = 1; i < selects.length; i++) {
            switch (selects[i]) {
                case "from":
                    fromIndex = i;
                    break;
                case "where":
                    whereIndex = i;
                    break;
                case "group":
                    groupIndex = i;
                    break;
                case "order":
                    orderIndex = i;
                    break;
                default:
                    break;
            }
        }

        init();

        // 多表连接操作
        // 额外的map键值对缓存
        Map<String, Object> mapTemp = new HashMap<String, Object>();
        for (int i = 0; i < froms.size(); i++) {
            // 先判断表是否存在
            File file = new File(DBMS.DATA_PATH + froms.get(i) + ".dbf");
            if (!file.exists()) {
                // 表不存在，抛出异常
                throw new MyException("表不存在");
            }
            DBFContent content = DBFUtils.getFileData(froms.get(i));
            if (i == 0)
                this.content = content;
            else {
                for (Map<String, Object> map : content.getContents()) {
                    for (Map<String, Object> mapOfTotal : this.content
                            .getContents()) {// 分别循环遍历两个表，找到相同属性名且值相同，进行连接操作，结果存在第一个表中
                        mapTemp = new HashMap<String, Object>();
                        for (Map.Entry<String, Object> entry : mapOfTotal
                                .entrySet()) {
                            if (map.containsKey(entry.getKey())) {
                                for (Map.Entry<String, Object> entryOfExtra : map
                                        .entrySet()) {
                                    if (!mapOfTotal.containsKey(entryOfExtra
                                            .getKey())) {
                                        mapTemp.put(entryOfExtra.getKey(),
                                                entryOfExtra.getValue());
                                        motifyDBFFields(content.getFields(),
                                                entryOfExtra);
                                    }
                                }
                            }
                        }
                        for (Map.Entry<String, Object> entry : mapTemp
                                .entrySet())
                            mapOfTotal.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        // 连接操作之后，重新初始化查询字段内容
        initArgs();
    }

    // 连接操作，添加因连接操作而增加的字段
    private void motifyDBFFields(List<DBFField> list,
                                 Map.Entry<String, Object> entry) {
        if (!this.content.containField(entry.getKey())) {
            this.content.getFields().add(
                    list.get(getIndexOfDBFField(list, entry.getKey())));
            this.content.setFieldCount(this.content.getFieldCount() + 1);
        }
    }

    // 得到字段名在字段数组中的下标位置
    private int getIndexOfDBFField(List<DBFField> list, String key) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getName().equals(key))
                return i;
        }
        return -1;
    }

    public DBFContent excuteSQL() throws MyException {// 执行语句
        // 得到结果集
        DBFContent resultContent = new DBFContent();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (int j = 0; j < content.getRecordCount(); j++) {
            int flag = 0;
            for (int k = 0; conditions == null || k < conditions.size(); k++) {
                System.out.println("j = " + j);
                if (conditions == null) {
                    flag = 1;
                    break;
                }
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
                // 存储符合Where条件的记录，等待输出
                list.add(content.getContents().get(j));
            } else {
                System.out.println(content.getContents().get(j));
            }
        }
        System.out.println(content);

        // 排序,先排后面的字段，再排前面的字段，实现多字段排序
        int orderType = Comparer.ASC;
        if (orders != null && orders.size() > 0) {
            for (int i = orders.size() - 1; i >= 0; i--) {
                // 根据字段名和升序降序生成比较器，进行排序操作
                switch (orders.get(i)) {
                    case "asc":
                        orderType = Comparer.ASC;
                        break;
                    case "desc":
                        orderType = Comparer.DESC;
                        break;
                    default:
                        Collections.sort(list,
                                new Comparer(orderType, orders.get(i)));
                        orderType = Comparer.ASC;// 恢复初始化值
                        break;
                }
            }
        }

        resultContent.setContents(list);
        resultContent.setFieldCount(content.getFieldCount());
        resultContent.setFields(content.getFields());
        resultContent.setRecordCount(list.size());

        for (int i = 0; i < resultContent.getContents().size(); i++) {
            System.out
                    .println("result = " + resultContent.getContents().get(i));
        }
        System.out.println(args);
        return resultContent;
    }

    // 初始化
    private void init() throws MyException {
        initArgs();
        initFrom();
        if (whereIndex > 0) {
            initWhere();
        }
        if (groupIndex > 0) {
            initGroup();
        }
        if (orderIndex > 0) {
            initOrder();
        }
    }

    // 读取Order语句块，解析存到Orders数组中
    private void initOrder() {
        orders = new ArrayList<>();
        for (int i = orderIndex + 2; i < originalSql.length; i++) {
            if (originalSql[i].equals(","))
                continue;
            orders.add(originalSql[i]);
        }
    }

    // 读取Group语句块，解析存到Group数组中
    private void initGroup() {
        groups = new ArrayList<>();
        if (orderIndex > 0) {
            for (int i = groupIndex + 2; i < orderIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                groups.add(originalSql[i]);
            }
        } else {
            for (int i = groupIndex + 2; i < originalSql.length; i++) {
                if (originalSql[i].equals(","))
                    continue;
                groups.add(originalSql[i]);
            }
        }
    }

    // 将Where字符串数组中的条件提取到wheres数组中
    private void initWhere() throws MyException {
        wheres = new ArrayList<>();
        if (groupIndex > 0) {
            for (int i = whereIndex + 1; i < groupIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                wheres.add(originalSql[i]);
            }
        } else if (orderIndex > 0) {
            for (int i = whereIndex + 1; i < orderIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                wheres.add(originalSql[i]);
            }
        } else {
            for (int i = whereIndex + 1; i < originalSql.length; i++) {
                if (originalSql[i].equals(","))
                    continue;
                wheres.add(originalSql[i]);
            }
        }
        translateWhere();
    }

    // 根据wheres数组解析字符串，合成OR条件组
    private void translateWhere() throws MyException {// froms.get()多表查询
        conditions = new ArrayList<>();
        where_or or = new where_or(froms.get(0));
        for (int i = 0; i < wheres.size(); i++) {
            String w1 = wheres.get(i);
            switch (w1) {
                case "between":
                    where_and and1 = new where_and(froms.get(0),
                            content.getFields());
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
                    or = new where_or(froms.get(0));
                    break;
                default:
                    where_and andOfWhere = new where_and(froms.get(0),
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
                        throw new MyException("该条件语句含有该数据库不支持的比较条件");
                    }
                    break;
            }
        }
        conditions.add(or);
    }

    // 根据比较方式提取条件中的值进行比较
    public void singleAndTranslate(where_or or, String w1,
                                   where_and andOfWhere, String type) throws MyException {
        String[] temp = w1.split(type);
        if (temp.length != 2) {
            // 参数数目不正确，抛出异常
            System.out.println(" 参数数目不正确，抛出异常");
            throw new MyException("条件语句比较参数数目不正确");
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

    // 提取要查询的表名，from后面的语句块有可能不确定，根据不同情况进行提取
    private void initFrom() {
        froms = new ArrayList<>();
        if (whereIndex > 0) {
            for (int i = fromIndex + 1; i < whereIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                froms.add(originalSql[i]);
            }
        } else if (groupIndex > 0) {
            for (int i = fromIndex + 1; i < groupIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                froms.add(originalSql[i]);
            }
        } else if (orderIndex > 0) {
            for (int i = fromIndex + 1; i < orderIndex; i++) {
                if (originalSql[i].equals(","))
                    continue;
                froms.add(originalSql[i]);
            }
        } else {
            for (int i = fromIndex + 1; i < originalSql.length; i++) {
                if (originalSql[i].equals(","))
                    continue;
                froms.add(originalSql[i]);
            }
        }
    }

    // 提取要查询的属性
    private void initArgs() {
        args = new ArrayList<>();
        for (int i = 1; i < fromIndex; i++) {
            if (originalSql[i].contains("(")) {
                // 聚合函数，未实现

            } else if (originalSql[i].equals("*")) {// 查询*表示查询所有字段
                for (DBFField field : content.getFields()) {
                    Args arg = new Args();
                    arg.type = Args.NORMAL;
                    arg.argsName = field.getName();
                    args.add(arg);
                }
            } else {// 根据字符串中的数据生成查询属性数组
                Args arg = new Args();
                arg.type = Args.NORMAL;
                arg.argsName = originalSql[i];
                args.add(arg);
            }
        }
    }

    // 排序比较器
    public class Comparer implements Comparator<Map<String, Object>> {

        public static final int ASC = 1001;
        public static final int DESC = 1002;
        private int type;
        private String field;

        public Comparer(int type, String field) {
            this.type = type;
            this.field = field;
        }

        @Override
        public int compare(Map<String, Object> o1, Map<String, Object> o2) {
            try {
                switch (type) {
                    case ASC:
                        double v1 = Double.parseDouble((String) o1.get(field));
                        double v2 = Double.parseDouble((String) o2.get(field));
                        if (v1 > v2)
                            return 1;
                        else if (v1 < v2)
                            return -1;
                        else
                            return 0;
                    case DESC:
                        double vv1 = Double.parseDouble((String) o1.get(field));
                        double vv2 = Double.parseDouble((String) o2.get(field));
                        if (vv1 > vv2)
                            return -1;
                        else if (vv1 < vv2)
                            return 1;
                        else
                            return 0;
                    default:
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                // 转换出错，不是数字，抛出异常
            }
            return 0;
        }

    }

    // 查询字段，对常用聚合函数进行了定义
    public class Args {
        public Args() {
        }

        public int type;
        public String argsName;
        public int aggregateType;

        @Override
        public String toString() {
            return "Args [type=" + type + ", argsName=" + argsName
                    + ", aggregateType=" + aggregateType + "]";
        }

        public static final int NORMAL = 1;
        public static final int AGGREGATE = 2;
        public static final int AVG = 11;
        public static final int COUNT = 12;
        public static final int MAX = 13;
        public static final int MIN = 14;
        public static final int SUM = 15;
    }
}
