/**
 * Created by Nicolas on 03/11/2016.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.util.SystemClock;

public class HBaseTest
{

    private static Configuration conf = null;
    /**
     * Initialization
     */
    static
    {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] familys)
            throws Exception
    {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName))
        {
            System.out.println("table already exists!");
        } else
        {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++)
            {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception
    {
        try
        {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        }
        catch (MasterNotRunningException e)
        {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception
    {
        try
        {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException
    {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException
    {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);

        System.out.println("");
        for(KeyValue kv : rs.raw())
        {
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            //System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try
        {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss)
            {
                System.out.println("");
                for(KeyValue kv : r.raw())
                {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    //System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void newUser(String tableName) throws Exception
    {
        Scanner sc = new Scanner(System.in);
        String firstName, age, address, tel, bff; // Declaration of all the String needed
        String[] friends;

        System.out.print("First name: ");
        firstName = sc.nextLine(); // Get the first name
        System.out.print("Age: ");
        age = sc.nextLine(); // Get the age
        System.out.print("Address: ");
        address = sc.nextLine(); // Get the address
        System.out.print("Tel.: ");
        tel = sc.nextLine(); // Get the telephone number

        System.out.println("Who is your best friend?");
        bff = sc.nextLine(); // Get the name of the best friend

        System.out.println("Do you have any other friends ? (use a ',' for the separation between them)");
        friends = sc.nextLine().split(", "); // Get the name(s) of the other friend(s) by splitting the line using ", " as a splitter

        addRecord(tableName, firstName, "info", "age", age); // Add the record of the age
        addRecord(tableName, firstName, "info", "address", address); // Add the record of the address
        addRecord(tableName, firstName, "info", "tel", tel); // Add the record of the telephone number
        addRecord(tableName, firstName, "friends", "bff", bff); // Add the record of the best friend
        for (String friend : friends) // Add the record(s) of the other friend(s)
        {
            addRecord(tableName, firstName, "friends", "others", friend);
        }
    }

    public static void main(String[] agrs)
    {
        try
        {
            String tableName = "nbarbaste"; // Create a String for the table name
            String[] families = { "info", "friends" }; // Create an array of String for the column families
            HBaseTest.createTable(tableName, families); // Create a table with the name and the column families defined

            Scanner sc = new Scanner(System.in);
            int choice = 0; // Int used for doing the REPL
            String recordName; // String used to get the name of the record is case 2

            System.out.println("Welcome to FriendBook!");

            while(choice != 4) // While the user doesn't want to quit the program
            {
                System.out.println( "\n" +
                        "1 - Create a new user \n" +
                        "2 - Show the record of an existing user \n" +
                        "3 - Show the records of all existing users \n" +
                        "4 - Exit FriendBook");

                choice = sc.nextInt(); // Get the choice of the user

                switch(choice) // Switch based on the choice of the user
                {
                    case 1: newUser(tableName);
                        break;

                    case 2: System.out.println("Which record do you want to show?");
                        Scanner sc2 = new Scanner(System.in);
                        recordName = sc2.nextLine();
                        getOneRecord(tableName, recordName);
                        break;

                    case 3: getAllRecord(tableName);
                        break;

                    case 4: System.out.println("See you soon on FriendBook!");
                        break;

                    default: System.out.println("Invalid choice, please retry");
                        break;
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
