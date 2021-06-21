package MySQL;

import java.awt.Color;//抽象窗口工具包
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;
import javax.swing.*;
import com.linuxense.javadbf.DBFField;
import MySQL.DBFOP.DBFContent;

class MySQL {

    private DBMS dbms;
    private JTextArea showText, inputText;// 显示框与输入框
    private JPanel panel;// 面板容器
    private JFrame frame;// 窗口框架
    private JButton okButton;// 运行按钮
    private JButton clearButton;//reset按钮
    private static MySQL form;

    //使用单例模式创建一个窗口
    static{
        form = new MySQL();
        form.setVisible(true);//允许JVM可以根据数据模型执行paint方法开始画图
    }

    public static MySQL getInstance() {
        form = new MySQL();
        form.setVisible(true);
        return form;
    }

    private MySQL() {
        dbms = new DBMS(this);
        frame = new JFrame("DBMS-group6");
        frame.setSize(750, 600);
        frame.setBackground(Color.decode("#FFFFFF"));
        int windowWidth = frame.getWidth(); // 获得窗口宽
        int windowHeight = frame.getHeight(); // 获得窗口高
        Toolkit kit = Toolkit.getDefaultToolkit(); // 定义工具包
        Dimension screenSize = kit.getScreenSize(); // 获取屏幕的尺寸
        int screenWidth = screenSize.width; // 获取屏幕的宽
        int screenHeight = screenSize.height; // 获取屏幕的高
        frame.setLocation(screenWidth / 2 - windowWidth / 2, screenHeight / 2
                - windowHeight / 2);// 设置窗口居中显示

        init();
    }

    public void init() {
        panel = new JPanel();
        panel.setLayout(null);// 清空布局，使用像素位定义布局
        panel.setBackground(Color.decode("#FFFFFF"));

        showText = new JTextArea();
        showText.setBackground(Color.white);
        showText.setText("\n\n\n\n\t\tWelcome to use MySQL, you can try the following code to use the system:\n\n" +
                "      create table doctor(did char(6) primary key unique not null,dname char(10) not null,title char(10),department char(10),dage int);\n\n"+
        "      insert into doctor (did,dname,title,department,dage) values ('001','doctor1','主任','内科',40);\n" +
                "      insert into doctor (did,dname,title,department,dage) values ('002','doctor2','医生','外科',45);\n" +
                "      insert into doctor (did,dname,title,department,dage) values ('003','doctor3','主任','儿科',50);\n" +
                "      insert into doctor (did,dname,title,department,dage) values ('004','doctor4','副主任','神经科',38);\n" +
                "      insert into doctor (did,dname,title,department,dage) values ('005','doctor5','主任','放射科',42);\n" +
                "      insert into doctor (did,dname,title,department,dage) values ('006','doctor6','医生','骨科',34);\n\n"+
                "      alter table doctor add columnt int;\n\n"+
                "      delete from doctor where dage=34;\n\n" +
                "      update doctor set dname='doctorskr' where did='005';\n\n" +
                "      select * from doctor;\n\n" +
                "      select * from doctor where dage>=40 or title='主任'；\n\n" +
                "      drop table doctor\n");
        showText.setLineWrap(true);// 设置自动换行
        showText.setEditable(false);// 设置不可编辑
        showText.setSize(750, 450);// 设置大小
        showText.setLocation(0, 0);// 设置位置
        showText.setBackground(Color.decode("#FFFFFF"));
        panel.add(showText);

        JLabel label = new JLabel("Enter the SQL statement here:");
        label.setSize(750, 15);
        label.setLocation(280, 450);
        panel.add(label);

        inputText = new JTextArea();
        inputText.setBackground(Color.decode("#FAFAFA"));
        inputText.setSize(750, 60);
        inputText.setLineWrap(true);
        inputText.setLocation(0, 462);
        panel.add(inputText);

        okButton = new JButton("Run");
        okButton.setLocation(260, 525);
        okButton.setSize(60, 35);
        okButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String sql = inputText.getText();
                if (sql == null || sql.length() == 0 || sql.trim().equals("")) {
                    showText.setText("Please do not enter an empty SQL statement!");
                } else {
                    try {
                        dbms.parseSQL(sql);
                    } catch (MyException e1) {
                        showText.setText(e1.ex);
                    }
                }
            }
        });
        panel.add(okButton);

        clearButton = new JButton("Reset");
        clearButton.setLocation(400,525);
        clearButton.setSize(70,35);
        clearButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                inputText.setText("");
            }
        });
        panel.add(clearButton);

        frame.add(panel);// 设置窗口框架容器面板
    }

    public static void main(String[] args) {
        try {
            Class.forName("MySQL.MySQL");//要求JVM查找并加载指定的类，也就是说JVM会执行该类的静态代码段。
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setOutput(String output) {
        showText.setText("------------------------------------------------------------------------------------------"
                + output
                + "------------------------------------------------------------------------------------------");
    }

    // 设置窗口输出
    public void setOutput(DBFContent content, String title) {
        StringBuilder builder = new StringBuilder();
        if (title != null) {
            builder.append(
                    "------------------------------------------------------------------------------------------")
                    .append(title)
                    .append("--------------------------------------------------------------------------------------\n");
        }
        builder.append("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n");
        List<DBFField> fields = content.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i == fields.size() - 1) {
                builder.append("      ").append(fields.get(i).getName())
                        .append("\n");
            } else
                builder.append("      ").append(fields.get(i).getName())
                        .append("       |");
        }
        builder.append("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n");
        for (int i = 0; i < content.getRecordCount(); i++) {
            Map<String, Object> map = content.getContents().get(i);
            for (int j = 0; j < fields.size(); j++) {
                if (j == fields.size() - 1) {
                    builder.append("     ")
                            .append(map.get(fields.get(j).getName()))
                            .append("\n");
                } else
                    builder.append("     ")
                            .append(map.get(fields.get(j).getName()))
                            .append("     |");
            }
            builder.append("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n");
        }
        showText.setText(builder.toString());
    }

    // 清空输入框
    public void clearInput() {
        inputText.setText("");
    }

    // 显示窗口
    public void setVisible(boolean b) {
        frame.setVisible(b);
    }

    // 接收异常，在窗口输出
    public void receiveException(String e) {
        showText.setText(e);
    }
}
