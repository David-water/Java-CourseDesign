package MySQL.DBFOP;

import java.util.*;
import com.linuxense.javadbf.DBFField;

//存储从DBF文件读取到的数据库数据
public class DBFContent {

    private String tableName;// 表名
    private int fieldCount, recordCount;// 字段数，记录数
    private List<Map<String, Object>> contents;// 读取的DBF文件内容
    private List<DBFField> fields;// DBF文件的字段

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DBFContent() {
        contents = new ArrayList<Map<String, Object>>();
        fields = new ArrayList<DBFField>();
    }

    public DBFContent(int fieldCount, int recordCount,
                      List<Map<String, Object>> contents, List<DBFField> fields) {
        this.fieldCount = fieldCount;
        this.recordCount = recordCount;
        this.contents = contents;
        this.fields = fields;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public List<Map<String, Object>> getContents() {
        return contents;
    }

    public void setContents(List<Map<String, Object>> contents) {
        this.contents = contents;
    }

    public List<DBFField> getFields() {
        return fields;
    }

    public void setFields(List<DBFField> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "DBFContent [fieldCount=" + fieldCount + ", recordCount="
                + recordCount + ", contents=" + contents + ", fields=" + fields
                + "]";
    }

    // 检查是否存在某字段
    public boolean containField(String key) {
        for (DBFField field : fields) {
            if (field.getName().equals(key))
                return true;
        }
        return false;
    }

}

