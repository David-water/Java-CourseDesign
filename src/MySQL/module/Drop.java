package MySQL.module;

import java.io.*;
import MySQL.DBMS;
import MySQL.MyException;

public class Drop {
    private String tableName;
    public Drop(String tableName){
        this.tableName = tableName;
    }
    public String excuteSQL() throws MyException {
        File file = new File(DBMS.DATA_PATH + tableName + ".dbf");
        if (!file.exists()) {
            // 表不存在，抛出异常
            throw new MyException("表不存在");
        }
        file.delete();
        return "删除成功";
    }
}
