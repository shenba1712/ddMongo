import com.mongodb.DB;

import java.io.*;
import java.util.*;
import java.util.List;


public class TransactionPicker extends Thread {
    private Thread t;
    private int filenum;
    private String keyspace;
    private double startTime;
    private double endTime;
    private String dir;
    private DB db=MongoJDBC.db;
    BufferedWriter bw=new BufferedWriter( new FileWriter(new File("C:\\Users\\Toshiba\\Desktop\\output.txt")));


    TransactionPicker(int filenum, String keyspace) throws IOException {
        this.filenum = filenum;
        this.keyspace = keyspace;
        this.dir=keyspace;
        this.startTime = System.currentTimeMillis();
    }

    // Function to read the file and call transactions
    private int readFile() throws IOException {
        int count = 0;
        BufferedReader br = null;

        try {
            String filename = "C:\\Users\\Toshiba\\Desktop\\D8-xact-revised-b\\"+String.valueOf(filenum) + ".txt";
           // String filename = "/temp/mongodb/xact/"+dir+"/"+String.valueOf(filenum) + ".txt";
            System.out.println(filename);
            Xact transaction = new Xact(db);
            br = new BufferedReader(new FileReader(filename));

            String line;

            // Copied data from file to list
            while ((line = br.readLine()) != null) {
                String[] trans=line.split(",");
                if(trans[0].equals("N")) {
                    int c_id = Integer.parseInt(trans[1]);
                    int w_id = Integer.parseInt(trans[2]);
                    int d_id = Integer.parseInt(trans[3]);
                    int m = Integer.parseInt(trans[4]); //no of orderline
                    // read the items in the new order
                    if (m > 20) {
                        System.out.println("Cannot Process Order for " + c_id + " as number of items exceed 20.");
                    } else {
                        int[] item_num = new int[m];
                        int[] s_w_id = new int[m];
                        int[] quantity = new int[m];
                        for(int j=0; j<m; j++){
                            line=br.readLine();
                            String [] values=line.split(",");
                            item_num[j] = Integer.parseInt(values[0]);
                            s_w_id[j] = Integer.parseInt(values[1]);
                            quantity[j] = (Integer.parseInt(values[2]));
                        }
                        String output=transaction.newOrder(w_id, d_id, c_id, m, item_num, s_w_id, quantity);
                        bw.write("\n\n"+output);
                        count++;
                    }
                }
                else if ((trans[0]).equals("P")) {
                    String output=transaction.payment(Integer.parseInt(trans[1]), Integer.parseInt(trans[2])
                            , Integer.parseInt(trans[3]), Double.parseDouble(trans[4]));
                    bw.write("\n\n"+output);
                    count++;
                } else if ((trans[0]).equals("D")) {
                    // Delivery transaction -> W_ID, CARRIER_ID
                    String output=transaction.delivery(Integer.parseInt(trans[1]),
                            Integer.parseInt(trans[2]));
                    bw.write("\n\n"+output);
                    count++;
                } else if ((trans[0]).equals("O")) {
                    // Order Status transaction -> W_ID, D_ID, C_ID
                    String output=transaction.orderStatus(Integer.parseInt(trans[1]),
                            Integer.parseInt(trans[2]),
                            Integer.parseInt(trans[3]));
                    bw.write("\n\n"+output);
                    count++;
                } else if ((trans[0]).equals("S")) {
                    // Stock level transaction -> W_ID, D_ID, T, L
                    String output=transaction.stockLevel(Integer.parseInt(trans[1]),
                            Integer.parseInt(trans[2]),
                            Integer.parseInt(trans[3]),
                            Integer.parseInt(trans[4]));
                    bw.write("\n\n"+output);
                    count++;
                }  else if ((trans[0]).equals("I")) {
                    // Popular Item transaction -> W_ID, D_ID, L
                    String output=transaction.popularItem(Integer.parseInt(trans[1]),
                            Integer.parseInt(trans[2]),
                            Integer.parseInt(trans[3]));
                    bw.write("\n\n"+output);
                    count++;
                } else if(trans[0].equals("T")){
                    // Top Customer Balance
                    String output=transaction.topBalance();
                    bw.write("\n\n"+output);
                    count++;
                }
                else {
                    System.out.println("Invalid transaction");
                }
                }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    public void run() {
        int count = 0;
        try {
            count = this.readFile();
        } catch (Exception e) {
            System.out.println("File- " + filenum + " interrupted.");
        } finally {
            this.endTime = System.currentTimeMillis();
            double totalTransactionTime = (endTime - startTime)/1000;
            double throughput = count / totalTransactionTime;
            double avgTime = totalTransactionTime / count;
            System.err.println("File-" + filenum + " -> no of transactions:" + count + ", time: " + totalTransactionTime + "sec, throughput: "+ throughput + ", Average Time: " + avgTime+" sec");

            MongoJDBC.totalTransactions += count;
            MongoJDBC.throughputList.add(throughput);

            MongoJDBC.Filecount++;
            System.out.println(MongoJDBC.Filecount);

            if (MongoJDBC.Filecount == MongoJDBC.clientCount) {
                double totalTime = ((System.currentTimeMillis()) - MongoJDBC.startTime)/1000;

                double totalThroughput = MongoJDBC.totalTransactions / totalTime;
                double maxThroughput = Collections.max(MongoJDBC.throughputList);
                double minThroughput = Collections.min(MongoJDBC.throughputList);
                double avgThroughput = totalThroughput/MongoJDBC.clientCount;

                System.out.println("Total number of transactions processed : "+ MongoJDBC.totalTransactions);
                System.out.println("Total time for processing the transactions (in sec): " + totalTime);
                System.out.println("Transaction throughput: "+ totalThroughput);
                System.out.println("Maximum Throughput: " +maxThroughput);
                System.out.println("Minimum Throughput: "+minThroughput);
                System.out.println("Average Throughput: "+avgThroughput);

                System.err.println("Total number of transactions processed : "+ MongoJDBC.totalTransactions);
                System.err.println("Total time for processing the transactions (in sec): " + totalTime);
                System.err.println("Transaction throughput: "+ totalThroughput);
                System.err.println("Maximum Throughput: " +maxThroughput);
                System.err.println("Minimum Throughput: "+minThroughput);
                System.err.println("Average Throughput: "+avgThroughput);
                try {
                    bw.write("\nTotal number of transactions processed : "+ MongoJDBC.totalTransactions);
                    bw.write("\nTotal time for processing the transactions (in sec): " + totalTime);
                    bw.write("\nTransaction throughput: "+ totalThroughput);
                    bw.write("\nMaximum Throughput: " +maxThroughput);
                    bw.write("\nMinimum Throughput: "+minThroughput);
                    bw.write("\nAverage Throughput: "+avgThroughput);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    bw.flush();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void transactionPicker() {
        try {
            if (t == null) {
                t = new Thread(this);
                t.start();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
