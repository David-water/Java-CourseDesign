package MySQL;

public class MyException extends Throwable {
    public String ex;
    public MyException(String string){
        ex=string;
    }
    public static void throwException(String e){
        System.out.println(e);
    }
}
