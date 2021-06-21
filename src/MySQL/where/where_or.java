package MySQL.where;

import java.util.*;
import com.linuxense.javadbf.DBFField;
import MySQL.MyException;

public class where_or {
    private List<where_and> ands;//AND条件组
    private int count;
    private List<Boolean> status;//AND条件组中的真假状态
    private String table;//表名
    private boolean result;//整个OR条件组的真假状态

    public where_or(String table) {
        count = 0;
        status = new ArrayList<Boolean>();
        ands = new ArrayList<where_and>();
        this.table = table;
    }

    public void addAnd(where_and andOfWhere) {
        ands.add(andOfWhere);
        count++;
    }

    public boolean judgeCondition(Map<String, Object> record) throws MyException {
        int ff = 0;
        //判断OR中的每个AND条件组真假
        for (int i = 0; i < ands.size(); i++) {
            boolean flag = ands.get(i).judgeAnd(record);
            status.add(flag);
            if (!flag) {
                result = false;
                ff = 1;
                break;
            }
        }
        if (ff == 0)
            result = true;
        return result;
    }

    public List<where_and> getAnds() {
        return ands;
    }

    public void setAnds(List<where_and> ands) {
        this.ands = ands;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<Boolean> getStatus() {
        return status;
    }

    public void setStatus(List<Boolean> status) {
        this.status = status;
    }
}
