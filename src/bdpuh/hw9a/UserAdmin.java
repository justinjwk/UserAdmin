package bdpuh.hw9a;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class UserAdmin {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "User");

        if (args != null) {
        String command = args[0];

            // java UserAdmin add kss k.s@gmail.com mypasswd married 1970/06/03 “favorite color” “red”
            if (command.equals("add")) {

                String rowId = args[1];
                String email = args[2];
                String password = args[3];
                String status = args[4];
                String dob = args[5];
                String securityQuestion = args[6];
                String securityAnswer = args[7];

                Put put1 = new Put(toBytes(rowId));
                put1.add(toBytes("cred"), toBytes("email"), toBytes(email));
                put1.add(toBytes("cred"), toBytes("password"), toBytes(password));

                put1.add(toBytes("prefs"), toBytes("status"), toBytes(status));
                put1.add(toBytes("prefs"), toBytes("date_of_birth"), toBytes(dob));
                put1.add(toBytes("prefs"), toBytes("security_question"), toBytes(securityQuestion));
                put1.add(toBytes("prefs"), toBytes("security_answer"), toBytes(securityAnswer));

                hTable.put(put1);
            }
            // java UserAdmin delete kss
            else if (command.equals("delete")) {

                String rowID = args[1];

                Delete delete = new Delete(toBytes(rowID));
                hTable.delete(delete);

            }
            // java UserAdmin show kss
            else if (command.equals("show")) {

                String rowId = args[1];

                Get get = new Get(toBytes(rowId));
                Result result = hTable.get(get);
                print(result);

            }
            // java UserAdmin listall
            else if (command.equals("listall")) {

                Scan scan = new Scan();
                ResultScanner scanner = hTable.getScanner(scan);
                for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                    print(rr);
                }
            }
            // java UserAdmin login kss mypasswd 128.220.101.100
            else if (command.equals("login")) {
                String rowId = args[1];
                String password = args[2];
                String ip = args[3];

                Put put1 = new Put(toBytes(rowId));
                Get get = new Get(toBytes(rowId));
                Result result = hTable.get(get);
                byte[] passwd = result.getValue(toBytes("creds"), toBytes("password"));

                if (password.equals(Bytes.toString(passwd))) {
                    put1.add(toBytes("lastlogin"), toBytes("success"), toBytes("yes"));
                }
                else {
                    put1.add(toBytes("lastlogin"), toBytes("success"), toBytes("yes"));
                }

                put1.add(toBytes("lastlogin"), toBytes("ip"), toBytes(ip));
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
                Date date = new Date();
                put1.add(toBytes("lastlogin"), toBytes("date"), toBytes(dateFormat.format(date)));
                dateFormat = new SimpleDateFormat("HH:mm:ss");
                put1.add(toBytes("lastlogin"), toBytes("time"), toBytes(dateFormat.format(date)));

            }
        }

        hTable.close();
    }

    private static void print(Result result) {
        System.out.println("rowid=" + Bytes.toString(result.getRow()));
        byte[] val1 = result.getValue(toBytes("creds"), toBytes("email"));
        System.out.println("creds:email=" + Bytes.toString(val1));
        byte[] val2 = result.getValue(toBytes("creds"), toBytes("password"));
        System.out.println("creds:password=" + Bytes.toString(val2));
        byte[] val3 = result.getValue(toBytes("prefs"), toBytes("status"));
        System.out.println("prefs:status=" + Bytes.toString(val3));
        byte[] val4 = result.getValue(toBytes("prefs"), toBytes("date_of_birth"));
        System.out.println("prefs:date_of_birth=" + Bytes.toString(val4));
        byte[] val5 = result.getValue(toBytes("prefs"), toBytes("security_question"));
        System.out.println("prefs:security_question=" + Bytes.toString(val5));
        byte[] val6 = result.getValue(toBytes("prefs"), toBytes("security_answer"));
        System.out.println("prefs:security_answer=" + Bytes.toString(val6));
        byte[] val7 = result.getValue(toBytes("last_login"), toBytes("ip"));
        System.out.println("last_login:ip=" + Bytes.toString(val7));
        byte[] val8 = result.getValue(toBytes("last_login"), toBytes("date"));
        System.out.println("last_login:date=" + Bytes.toString(val8));
        byte[] val9 = result.getValue(toBytes("last_login"), toBytes("time"));
        System.out.println("last_login:time=" + Bytes.toString(val9));
        byte[] val10 = result.getValue(toBytes("last_login"), toBytes("success"));
        System.out.println("last_login:success=" + Bytes.toString(val10));
    }
}
