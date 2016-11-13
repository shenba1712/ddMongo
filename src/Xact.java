import java.util.*;
import java.text.SimpleDateFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class Xact {
    private DB db;
    private DBCollection wd;
    private DBCollection is;
    private DBCollection ol;
    private DBCollection customer;

    Xact(DB db) {
        this.db=db;
        wd =db.getCollection("dw");
        is=db.getCollection("is");
        ol=db.getCollection("ool");
      //  wd=db.getCollection("district_warehouse");
       // is=db.getCollection("item_stock");
       // ol=db.getCollection("order_orderline");
        customer =db.getCollection("customer");
    }

    String newOrder(int W_ID, int D_ID, int C_ID, int NUM_ITEMS, int[] ITEM_NUMBER, int[] S_W_ID,
                  int[] QUANTITY) {

        double o_id = 0;
        double Qnty = 0;
        long a_qty[] = new long[NUM_ITEMS];

        double OL_CNT = NUM_ITEMS;
        double O_ALL_LOCAL = 1;
        double TOTAL_AMOUNT = 0;
        double stockQty[] = new double[NUM_ITEMS];
        double itemPrice[] = new double[NUM_ITEMS];
        double ADJUSTED_QTY[] = new double[NUM_ITEMS];
        long remoteCnt = 0;
        double ITEM_AMOUNT[] = new double[NUM_ITEMS];
        String itemName[] = new String[NUM_ITEMS];
        String OL_DIST_INFO = null;

        BasicDBObject whereQuery=new BasicDBObject("wid",W_ID);

        BasicDBObject districtFields=new BasicDBObject();
        districtFields.put("districtMap."+D_ID+".dNextoid",1);
        districtFields.put("districtMap."+D_ID+".dTax",2);
        districtFields.put("wTax",3);

        DBObject dbObject=wd.findOne(whereQuery,districtFields);
        if(dbObject==null)
            return "Transaction Completed!";
        Map districtDetails= dbObject.toMap();

        if(districtDetails==null)
            return "Transaction Done!";

        BasicDBObject updateDNextOID=new BasicDBObject();
        updateDNextOID.put("$inc",new BasicDBObject("districtMap."+D_ID+".dNextoid",1));
        wd.update(whereQuery,updateDNextOID);


        Map d1= (Map) districtDetails.get("districtMap");
        Map d2=(Map) d1.get(String.valueOf(D_ID));
        if(d2==null)
            return "Transaction Done!";

        BasicDBObject customerCriteria=new BasicDBObject();
        customerCriteria.put("wid",W_ID);
        customerCriteria.put("did",D_ID);
        customerCriteria.put("cid",C_ID);

        BasicDBObject customerFields=new BasicDBObject();
        customerFields.put("wid",1);
        customerFields.put("did",2);
        customerFields.put("cid",3);
        customerFields.put("cName",4);
        customerFields.put("cDiscount",5);
        customerFields.put("cCredit",6);

        DBObject dbObject2= customer.findOne(customerCriteria,customerFields);
        if (dbObject2==null)
            return "Transaction Completed!";
        Map c2= dbObject2.toMap();

        if(c2==null)
            return "Transaction Done!";

        String customerName[]=c2.get("cName").toString().split("_");

        String output = "New order Transaction Details: W_ID: " + W_ID + ", D_ID: " + D_ID + ", C_ID: " + C_ID + ", LastName: "
                + customerName[2] + ", Credit: " + c2.get("cCredit").toString() + ", Discount: " + c2.get("cDiscount").toString()
                + " Warehouse tax rate: " + districtDetails.get("wTax").toString() + ", District tax rate: " + d2.get("dTax").toString()
                + " Order number: " + d2.get("dNextoid").toString()+ ", Date: " + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date());

        o_id= Double.parseDouble(d2.get("dNextoid").toString());

        BasicDBObject ol1=new BasicDBObject();
        BasicDBObject oInsert=new BasicDBObject();
        oInsert.put("wid",W_ID);
        oInsert.put("did",D_ID);
        oInsert.put("oid",o_id);
        oInsert.put("cid",C_ID);
        oInsert.put("oCarrierId",0);
        oInsert.put("oEntryd",new Date());
        oInsert.put("first",customerName[0]);
        oInsert.put("middle",customerName[1]);
        oInsert.put("last",customerName[2]);
        oInsert.put("olCnt",OL_CNT);
        oInsert.put("oAllLocal",O_ALL_LOCAL);

        // check whether supplier warehouse is local or remote
        for (int i = 0; i < NUM_ITEMS; i++) {
            if (S_W_ID[i] != W_ID) {
                O_ALL_LOCAL = 0;
                break;
            }
        }

        for(int i=0; i<NUM_ITEMS; i++){
            BasicDBObject stockQuantityConditions=new BasicDBObject().append("iid",ITEM_NUMBER[i]);
            BasicDBObject stockQuantity=new BasicDBObject();
            stockQuantity.put("stockMap."+S_W_ID[i]+".sQuantity",1);
            stockQuantity.put("stockMap."+S_W_ID[i]+".sYtd",2);
            stockQuantity.put("stockMap."+S_W_ID[i]+".sOrderCnt",3);
            stockQuantity.put("stockMap."+S_W_ID[i]+".sRemoteCnt",4);
            stockQuantity.put("iName",5);
            stockQuantity.put("iPrice",6);
            stockQuantity.put("stockMap."+S_W_ID[i]+".sDist01",7);

            DBObject dbO=is.findOne(stockQuantityConditions,stockQuantity);
            if(dbO==null)
                return "Transaction done!";
            Map stckQty=dbO.toMap();

            if (stckQty==null)
                return "Transaction Done!";

            Map s1= (Map) stckQty.get("stockMap");
            Map s2=(Map) s1.get(String.valueOf(S_W_ID[i]));
            if(s2==null)
                return "Transaction Done!";
            stockQty[i]= (double) s2.get("sQuantity");
            OL_DIST_INFO=s2.get("sDist01").toString().split("_")[D_ID-1];

            ADJUSTED_QTY[i] = stockQty[i] - QUANTITY[i];

            if(ADJUSTED_QTY[i] < 10) {
                ADJUSTED_QTY[i] += 100;
                a_qty[i]=QUANTITY[i]-100;
            }
            else
                a_qty[i]=QUANTITY[i];

            if (S_W_ID[i] != W_ID) {
                remoteCnt = 1;
            }

            BasicDBObject updateStockCriteria=new BasicDBObject();
            updateStockCriteria.put("iid",ITEM_NUMBER[i]);
            BasicDBObject updateStock=new BasicDBObject();
            updateStock.put("stockMap."+S_W_ID[i]+".sQuantity",1);
            updateStock.put("stockMap."+S_W_ID[i]+".sYtd",2);
            updateStock.put("stockMap."+S_W_ID[i]+".sOrderCnt",3);
            updateStock.put("stockMap."+S_W_ID[i]+".sRemoteCnt",4);
            DBObject dbo1=is.findOne(updateStockCriteria,updateStock);
            if(dbo1==null)
                return "Transaction Done!";
            Map stockDetails=dbo1.toMap();
            Map stock1=(Map) stockDetails.get("stockMap");
            Map stock2=(Map) stock1.get(String.valueOf(S_W_ID[i]));

            double sQuantity= Double.valueOf(stock2.get("sQuantity").toString())-a_qty[i];
            double sYtd= Double.valueOf(stock2.get("sYtd").toString())+QUANTITY[i];
            double sOrderCnt=  Double.valueOf(stock2.get("sOrderCnt").toString())+1;
            double sRemoteCnt= Double.valueOf(stock2.get("sRemoteCnt").toString())+remoteCnt;
            updateStock.clear();
            updateStock.put("stockMap."+S_W_ID[i]+".sQuantity",sQuantity);
            updateStock.put("stockMap."+S_W_ID[i]+".sYtd",sYtd);
            updateStock.put("stockMap."+S_W_ID[i]+".sOrderCnt",sOrderCnt);
            updateStock.put("stockMap."+S_W_ID[i]+".sRemoteCnt",sRemoteCnt);
            is.update(updateStockCriteria,new BasicDBObject("$set",updateStock));

            itemName[i]= (String) stckQty.get("iName");
            itemPrice[i]= (double) stckQty.get("iPrice");
            ITEM_AMOUNT[i] = QUANTITY[i] * itemPrice[i];
            TOTAL_AMOUNT += ITEM_AMOUNT[i];

            BasicDBObject olInsert=new BasicDBObject();
            olInsert.clear();
            olInsert.put("olNumber",i+1);
            olInsert.put("iid",ITEM_NUMBER[i]);
            olInsert.put("iName",itemName[i]);
            olInsert.put("iPrice",itemPrice[i]);
            olInsert.put("olAmt",ITEM_AMOUNT[i]);
            olInsert.put("olQty",QUANTITY[i]);
            olInsert.put("olDeliveryd","2000-01-01 00:00:00+0000");
            olInsert.put("olSupplywid",S_W_ID[i]);
            olInsert.put("olDistInfo",OL_DIST_INFO);

            ol1.put(String.valueOf(i+1),olInsert);

            output = output + "\n ITEM_NUMBER: " + ITEM_NUMBER[i] + ", I_NAME: " + itemName[i] + ", Supplier Warehouse ID: "
                    + S_W_ID[i] + ", QUANTITY: " + QUANTITY[i] + ", OL_AMOUNT: " + ITEM_AMOUNT[i] + ", S_QUANTITY: "
                    + stockQty[i];
        }
        oInsert.put("orderline",ol1);
        ol.insert(oInsert);
         TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + (double)d2.get("dTax") + (double)districtDetails.get("wTax")) * (1 - (Double)c2.get("cDiscount"));
         output += "\n NUM_ITEMS: " + NUM_ITEMS + ", Total amount for order: " + TOTAL_AMOUNT;
        return output;
    }

    String topBalance(){
        List<Map> topBalCustDetails=new ArrayList();
        DBCursor customerDetails= customer.find().sort(new BasicDBObject("cBalance",-1)).limit(10);

        if(customerDetails.size()==0)
            return "Transaction Done!";

        BasicDBObject districtCriteria=new BasicDBObject();
        BasicDBObject districtFields=new BasicDBObject();

        while(customerDetails.hasNext()){
            topBalCustDetails.add((Map) customerDetails.next());
        }
        String output="Top Balance Transaction Details: \nThe top 10 customers with outstanding balance are:";
        for(Map m:topBalCustDetails){
            int wid= (int) m.get("wid");
            int did=(int) m.get("did");
            districtCriteria.put("wid",wid);
            districtFields.put("wName",1);
            districtFields.put("districtMap."+did+".dName",2);
            DBObject db=wd.findOne(districtCriteria,districtFields);
            if(db==null)
                return "Transaction done!";
            Map district=db.toMap();
            Map d1=(Map) district.get("districtMap");
            Map d2=(Map) d1.get(String.valueOf(did));
                String[] names=m.get("cName").toString().split("_");
                String name= names[0].concat(" ").concat(names[1]).concat(" ").concat(names[2]);
                output+="Name: "+name+", Balance: "+m.get("cBalance")+", " +
                        "Warehouse Name: "+district.get("wName").toString() +
                        ", District Name: "+d2.get("dName").toString()+"\n";
        }
        return output;
    }

    String popularItem(int wid, int did, int l) {
        String output = "Popular Transaction Details:  w_id:" + wid + ", d_id:" + did + ", Number of last orders to be examined:" + l;
        BasicDBObject whereQuery = new BasicDBObject().append("wid", wid);

        BasicDBObject fields = new BasicDBObject();
        fields.put("districtMap." + did + ".dNextoid", 1);

        DBObject d=wd.findOne(whereQuery, fields);
        if(d==null)
            return "Transaction Done!";
        Map d1 = d.toMap();
        if(d1==null)
            return "Transaction Done!";

        Map d2 = (Map) d1.get("districtMap");
        Map d3 = (Map) d2.get(String.valueOf(did));
        if(d3==null)
            return "Transaction Done!";

        double N = Double.parseDouble(d3.get("dNextoid").toString());
        double range = N - l;


        BasicDBObject orderCriteria = new BasicDBObject();
        orderCriteria.put("oid", new BasicDBObject("$gte", range));
        orderCriteria.put("wid", wid);
        orderCriteria.put("did", did);

        BasicDBObject orderFields = new BasicDBObject();
        orderFields.put("oid", 1);
        orderFields.put("wid", 7);
        orderFields.put("did", 8);
        orderFields.put("orderline", 2);
        orderFields.put("oEntryd", 3);
        orderFields.put("first", 4);
        orderFields.put("last", 5);
        orderFields.put("middle", 6);

        List<Map> orderOrderlineDetails = new ArrayList();
        DBCursor orderDetails = ol.find(orderCriteria, orderFields);

        if(orderDetails.size()==0)
            return "Transaction Done!";
        while (orderDetails.hasNext()) {
            orderOrderlineDetails.add((Map) orderDetails.next());
        }

        TreeSet<Double> oidTreeSet = new TreeSet<Double>();
        for (int i = 0; i < orderOrderlineDetails.size(); i++) {
            oidTreeSet.add(Double.valueOf(orderOrderlineDetails.get(i).get("oid").toString()));
        }
        Map<Integer, String> popularOIDs = new HashMap<Integer, String>();
        Iterator<Double> itr = oidTreeSet.iterator();

        while (itr.hasNext()) {
            double currentOid = itr.next();

            //storing the max quantity ordered for each oid
            int max = 0;
            for (int j = 0; j < orderOrderlineDetails.size(); j++) {

                Map map = (Map) orderOrderlineDetails.get(j).get("orderline");
                if(map==null)
                    break;

                if (currentOid == Double.valueOf (orderOrderlineDetails.get(j).get("oid").toString())) {
                    for (int i = 1; i <= map.size(); i++) {
                        Map m1 = (Map) map.get(String.valueOf(i));
                        if(m1==null)
                            return "Transaction Done!";
                        if (max < (int) m1.get("olQty"))
                            max = (int) m1.get("olQty");
                    }
                }
            }
            // printing the popular item for last L orders
            int i = 0;
            for (int j = 0; j < orderOrderlineDetails.size(); j++) {
                Map map = (Map) orderOrderlineDetails.get(j).get("orderline");
                if ((currentOid == Double.valueOf (orderOrderlineDetails.get(j).get("oid").toString()))) {
                    for (int k = 1; k <= map.size(); k++) {
                        Map m1 = (Map) map.get(String.valueOf(k));
                        if(m1==null)
                            break;
                        if (max == (int) m1.get("olQty")) {
                            if (i == 0) {
                                output += "\nOrder Number: " + currentOid + ", Entry date and time: " + orderOrderlineDetails.get(j).get("oEntryd")
                                        + ", Customer name" + orderOrderlineDetails.get(j).get("first").toString().concat(" ").concat(orderOrderlineDetails.get(j).get("middle").toString())
                                        .concat(" ").concat(orderOrderlineDetails.get(j).get("last").toString());
                            }
                            output += "\nItem Name: " + m1.get("iName") + ", Quantity Ordered: " + max;
                            popularOIDs.put((Integer) m1.get("iid"), m1.get("iName").toString());
                            i++;
                        }
                    }
                }
            }
        }
        output = output + "\n\n Percentage of orders in S that contain the popular items:";
        // printing orders that contain the popular item
        for (Map.Entry<Integer, String> entry : popularOIDs.entrySet()) {
            int iid = entry.getKey();
            String iname = entry.getValue();
            output = output + "\nItem Name: " + iname;
            int count = 0;
            double percentage = 0.00;
                for(int j = 0; j< orderOrderlineDetails.size(); j++) {
                    Map map = (Map) orderOrderlineDetails.get(j).get("orderline");
                    for (int i = 1; i <= map.size(); i++) {
                        Map m1 = (Map) map.get(String.valueOf(i));
                        if(m1==null)
                            break;
                        if ((iname.equals(m1.get("iName").toString())) && (iid == (int) m1.get("iid"))) {
                            count++;
                        }
                    }
                }
            percentage=((double) count*100)/(double) l;
            output = output + ", Number of orders in S containing the popular item: " + count;
            output = output + ", Percentage of orders in S containing the popular item: "+ percentage;
        }
        return output;
    }

    String stockLevel(int wid, int did, int t, int l){
        BasicDBObject whereQuery = new BasicDBObject().append("wid", wid);

        BasicDBObject fields = new BasicDBObject();
        fields.put("districtMap." + did + ".dNextoid", 1);

        DBObject d=wd.findOne(whereQuery,fields);
        if(d==null)
            return "Transaction Done!";
        Map d1 = d.toMap();
        Map d2 = (Map) d1.get("districtMap");
        Map d3 = (Map) d2.get(String.valueOf(did));
        if(d3==null)
            return "Transaction Done!";

        double N = Double.parseDouble(d3.get("dNextoid").toString());
        double range = N - l;

        BasicDBObject orderCriteria = new BasicDBObject();
        orderCriteria.put("oid", new BasicDBObject("$gte", range));
        orderCriteria.put("wid", wid);
        orderCriteria.put("did", did);

        BasicDBObject orderFields = new BasicDBObject();
        orderFields.put("oid", 1);
        orderFields.put("wid", 7);
        orderFields.put("did", 8);
        orderFields.put("orderline", 2);

        List<Map> orderOrderlineDetails = new ArrayList();
        DBCursor orderDetails = ol.find(orderCriteria, orderFields);

        if(orderDetails.size()==0)
            return "Transaction Done!";

        while (orderDetails.hasNext()) {
            orderOrderlineDetails.add((Map) orderDetails.next());
        }
        List<Integer> iidList=new ArrayList<>();
        for (int i=0; i<orderOrderlineDetails.size(); i++){
            Map map=(Map) orderOrderlineDetails.get(i).get("orderline");
            for(int j=1; j<=map.size(); j++){
                Map m1=(Map) map.get(String.valueOf(j));
                if(m1==null)
                    break;
                iidList.add((int)m1.get("iid"));
            }
        }

        BasicDBObject stockCriteria=new BasicDBObject();
        stockCriteria.put("iid",new BasicDBObject("$in",iidList));
        stockCriteria.put("stockMap."+wid+".sQuantity",new BasicDBObject("$lt",t));

        int count=0;
        count = is.find(stockCriteria).count();

        String output="Stock Transaction Details: w_id:" + wid + ", d_id:" + did + ", t:" + t
                + ", l:" + l + "\nTotal number of items where its stock quantity is below the threshold: " + count;
        return output;
    }

    String orderStatus(int wid, int did, int cid){
        BasicDBObject customerCriteria =new BasicDBObject();
        customerCriteria.put("wid",wid);
        customerCriteria.put("did",did);
        customerCriteria.put("cid",cid);
        BasicDBObject customerFields=new BasicDBObject();
        customerFields.put("cName",1);
        customerFields.put("cBalance",2);

        DBObject customerDetails= customer.findOne(customerCriteria,customerFields);
        if(customerDetails==null)
            return "Transaction Done!";
        Map c2=customerDetails.toMap();
        if(c2==null)
            return "Transaction Done!";

        String[] names=c2.get("cName").toString().split("_");
        String name= names[0].concat(" ").concat(names[1]).concat(" ").concat(names[2]);

        String output = "Order Status Details: ";
        output+= "Customer's Name : "+name+", Balance: "+c2.get("cBalance");

        BasicDBObject orderCriteria=new BasicDBObject();
        orderCriteria.put("wid",wid);
        orderCriteria.put("cid",cid);
        orderCriteria.put("did",did);
        BasicDBObject orderFields=new BasicDBObject();
        orderFields.put("cid",5);
        orderFields.put("oid",4);
        orderFields.put("oEntryd",1);
        orderFields.put("oCarrierId",2);
        orderFields.put("orderline",3);

        DBCursor order=ol.find(orderCriteria, orderFields);

        output="\nCustomer's last order:\n";
        while(order.hasNext()){
            Map o1=order.next().toMap();
            Map o2=(Map) o1.get("orderline");
            output+="Order Number: "+o1.get("oid")+", Entry date and time: "+o1.get("oEntryd")+
                    ", Carrier Identification: "+o1.get("oCarrierId");
            for(int i=1;i<=o2.size(); i++){
                Map o3=(Map) o2.get(String.valueOf(i));
                if(o3==null)
                    break;
                output+="\nItem Number: "+o3.get("iid")+", Item Name: "+o3.get("iName")+", Supplying warehouse ID: "+o3.get("olSupplywid")
                +", Quantity Ordered: "+o3.get("olQty")+", Total Amount: "+o3.get("olAmt")+", Date and Time of delivery: "
                +o3.get("olDeliveryd");
            }
        }
        return output;
    }

    String payment(int wid, int did, int cid, double pay){
        String output = "Payment Transaction Details: w_id: " + wid + ", d_id: " + did + ", c_id: " + cid
                + ", Payment amount: " + pay;

        BasicDBObject criteria=new BasicDBObject("wid",wid);
        BasicDBObject updateWarehouse=new BasicDBObject();
        updateWarehouse.put("wYtd",1);
        updateWarehouse.put("districtMap",3);
        updateWarehouse.put("districtMap."+did+".dYtd",2);
        DBObject d= wd.findOne(criteria,updateWarehouse);
        if(d==null)
            return "Transaction Done!";
        Map m1=d.toMap();
        Map d1=(Map) m1.get("districtMap");
        Map d2=(Map) d1.get(String.valueOf(did));
        double wYtd= Double.valueOf(m1.get("wYtd").toString())+pay;
        double dYtd=Double.parseDouble(d2.get("dYtd").toString())+pay;

        updateWarehouse.clear();
        updateWarehouse.put("wYtd",wYtd);
        updateWarehouse.put("districtMap."+did+".dYtd",dYtd);
        wd.update(criteria,new BasicDBObject("$set",updateWarehouse));

        BasicDBObject customerCriteria=new BasicDBObject();
        customerCriteria.put("wid",wid);
        customerCriteria.put("did",did);
        customerCriteria.put("cid",cid);
        BasicDBObject customerUpdateFields=new BasicDBObject();
        customerUpdateFields.put("cBalance",1);
        customerUpdateFields.put("cYtdPayment",2);
        customerUpdateFields.put("CPyttCnt",3);
        DBObject dbObject=customer.findOne(customerCriteria,customerUpdateFields);
        if(dbObject==null)
            return "Transaction done!";
        Map c1=dbObject.toMap();
        double cBalance= Double.valueOf(c1.get("cBalance").toString())+pay;
        double cYtdPayment=Double.valueOf(c1.get("cYtdPayment").toString())+pay;
        int cPytCnt=Integer.valueOf(c1.get("CPyttCnt").toString())+1;

        BasicDBObject newDoc=new BasicDBObject();
        newDoc.put("cBalance",cBalance);
        newDoc.put("cYtdPayment",cYtdPayment);
        newDoc.put("cPytCnt",cPytCnt);

        BasicDBObject update=new BasicDBObject();
        update.put("$set",newDoc);
        customer.update(customerCriteria,update);

        BasicDBObject districtFields=new BasicDBObject();
        districtFields.put("wid",1);
        districtFields.put("wAddress",2);
        districtFields.put("wName",12);
        districtFields.put("wYtd",15);
        districtFields.put("districtMap."+did+".dYtd",14);
        districtFields.put("districtMap."+did+".dName",13);
        districtFields.put("districtMap."+did+".dAddress",3);
        districtFields.put("districtMap",10);
        DBObject dbObject1=wd.findOne(criteria,districtFields);
        if(dbObject1==null)
            return "Transaction Done!";
        Map districtDetails= dbObject1.toMap();
        Map dis1=(Map)districtDetails.get("districtMap");
        Map dis2=(Map) dis1.get(String.valueOf(did));

        BasicDBObject customerFields=new BasicDBObject();
        customerFields.put("wid",1);
        customerFields.put("did",2);
        customerFields.put("cid",3);
        customerFields.put("cName",10);
        customerFields.put("cAddress",4);
        customerFields.put("cPhone",5);
        customerFields.put("cSince",6);
        customerFields.put("cCredit",7);
        customerFields.put("CCreditLimit",8);
        customerFields.put("cDiscount",9);
        DBObject dbObject2=customer.findOne(criteria,customerFields);
        if(dbObject2==null)
            return "Transaction Done!";
        Map c2= dbObject2.toMap();

        if(dis2==null)
            return "Transaction Done!";

        if(c2==null)
            return "Transaction Done!";

        String[] names=c2.get("cName").toString().split("_");
        String name= names[0].concat(" ").concat(names[1]).concat(" ").concat(names[2]);

        output+="\nCustomer Name: "+name+", Address: "+c2.get("cAddress").toString()+", Credit: "+c2.get("cCredit")
        +", Credit Limit: "+c2.get("CCreditLimit")+", Discount: "+c2.get("cDiscount")+", Balance: "
        +cBalance+"\nWarehouse Address: "+districtDetails.get("wAddress")
        +"\nDistrict Address: "+dis2.get("dAddress");

        return (output);
    }

    String delivery(int wid, int carrierId){
        for(int i=1; i<=10; i++){
            double oid=0;
            BasicDBObject orderCriteria=new BasicDBObject();
            orderCriteria.put("wid",wid);
            orderCriteria.put("did",i);
            orderCriteria.put("oCarrierId",0);
            BasicDBObject orderFields=new BasicDBObject();
            orderFields.put("cid",5);
            orderFields.put("oid",4);
            orderFields.put("oEntryd",1);
            orderFields.put("oCarrierId",2);
            orderFields.put("orderline",3);

            DBObject order=ol.findOne(orderCriteria, orderFields);
            if(order==null)
                break;
            Map map = order.toMap();
                 oid= Double.parseDouble(map.get("oid").toString());

                 BasicDBObject orderSelectCriteria=new BasicDBObject();
                 orderSelectCriteria.put("wid",wid);
                 orderSelectCriteria.put("did",i);
                 orderSelectCriteria.put("oid",oid);
                 orderSelectCriteria.put("oCarrierId",0);
                 BasicDBObject orderSelectFields=new BasicDBObject();
                 orderSelectFields.put("cid",1);
                 orderSelectFields.put("orderline",2);
                 DBObject dbObject10=ol.findOne(orderSelectCriteria,orderSelectFields);
                 if(dbObject10==null)
                     break;
                 Map orderSelect=dbObject10.toMap();
                 if(orderSelect==null)
                     return "Transction Done!";
                 Map m1=(Map) orderSelect.get("orderline");
                 double olNumber=0, olAmt=0;
                 int cid= (int) orderSelect.get("cid");
                 double balance=0;
                 for(int n=1; n<=m1.size(); n++){
                     Map m2=(Map) m1.get(String.valueOf(n));
                     if(m2==null)
                         break;
                     olNumber=Double.parseDouble(m2.get("olNumber").toString());
                     olAmt=Double.parseDouble(m2.get("olAmt").toString());
                     balance+=olAmt;
                 }

                 BasicDBObject orderUpdateCriteria=new BasicDBObject();
                 orderUpdateCriteria.put("wid",wid);
                 orderUpdateCriteria.put("did",i);
                 orderUpdateCriteria.put("oid",oid);
                 BasicDBObject orderUpdateField=new BasicDBObject();
                 orderUpdateField.put("oCarrierId",carrierId);
                 orderUpdateField.put("orderline."+olNumber+".olDeliveryd",new Date());
                 ol.update(orderUpdateCriteria,new BasicDBObject("$set",orderUpdateField));

                 BasicDBObject customerCriteria=new BasicDBObject();
                 customerCriteria.put("wid",wid);
                 customerCriteria.put("did",i);
                 customerCriteria.put("cid",cid);

                 BasicDBObject customerFields=new BasicDBObject();
                 customerFields.put("cBalance",1);
                 customerFields.put("CdeliveryCnt",2);
                 DBObject dbObject=customer.findOne(customerCriteria,customerFields);
                 if(dbObject==null)
                     return "Transaction Done!";
                 Map cus=dbObject.toMap();
                 double cBalance=Double.valueOf(cus.get("cBalance").toString())+balance;
                 long cDeliveryCnt=Integer.valueOf(cus.get("CdeliveryCnt").toString())+1;

                 customerFields.clear();
                 customerFields.put("cBalance",cBalance);
                 customerFields.put("CdeliveryCnt",cDeliveryCnt);
                 customer.update(customerCriteria,new BasicDBObject("$set",customerFields));
             }
        return "Delivery Occurred";
    }
}