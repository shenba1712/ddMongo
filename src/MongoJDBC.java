import com.mongodb.MongoClient;

import com.mongodb.DB;

import java.io.IOException;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

public class MongoJDBC {

    private static String keyspace;
    public static int totalTransactions=0;
    public static double sumTime=0.0;
    public static int Filecount =0;
    public static double startTime = 0.0;
    public static int clientCount=0;
    public static Set<Double> throughputList=new TreeSet<>() ;
    public static MongoClient mongoClient;
    public static DB db;

    public static void connect( String node, String k ) {

        try{
            // To connect to mongodb server
            mongoClient = new MongoClient(node);

            // Now connect to your databases
            db = mongoClient.getDB( k );
            System.out.println("Connect to database successfully");

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    public static void main(String[] args) throws IOException {
        String database=null;
        Integer node = 0;
        int startFile=0;
        int endFile=0;

        if(args.length==0) {
            Scanner s = new Scanner(System.in);
            System.out.println("Enter the keyspace: D8 or D40");
            database = s.next();
            if (!(database.equals("D8") || database.equals("D40"))) {
                System.out.println("Invalid database name. Please choose D8 or D40");
                System.exit(-1);
            }
            System.out.println("Enter no of nodes: 1 or 3");
            node = s.nextInt();
            if (!(node ==1 || node==3)) {
                System.out.println("Invalid number of nodes! Please choose 1 or 3");
                System.exit(-1);
            }

            // Validating the entered client number
            System.out.println("Enter the no of clients to run: 1 to 100");
            clientCount = s.nextInt();
            if (clientCount < 1 || clientCount > 100) {
                System.out.println("Invalid number of clients. Please choose a value between 1 to 100.");
                System.exit(-1);
            }
            endFile=clientCount;
        }
        else{
            database=args[0];
            node = Integer.parseInt(args[1]);
            startFile = Integer.parseInt(args[2]);
            endFile= Integer.parseInt(args[3]);
            clientCount=endFile;
        }

        if(database.equals("D8")&&node==1)
            keyspace="d8_db";
        else if(database.equals("D8")&&node==3)
            keyspace="d40";
        else if(database.equals("D40")&&node==1)
            keyspace="d8";
        else if(database.equals("D40")&&node==3)
            keyspace="d40";

        connect("127.0.0.1",keyspace);
        startTime = System.currentTimeMillis();

        for (int j = startFile; j<endFile; j++)
        {
            TransactionPicker tp = new TransactionPicker(j, keyspace);
            tp.transactionPicker();
        }
    }
}

