package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects;


import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.Utils;
import java.sql.ResultSet;
import java.sql.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Customer {

    private static final Logger logger = LogManager.getLogger();

    private Integer C_ID;
    private Integer C_D_ID;
    private Integer C_W_ID;
    private String C_LAST;
    private String C_MIDDLE;
    private String C_FIRST;
    private String C_STREET_1;
    private String C_STREET_2;
    private String C_CITY;
    private String C_STATE;
    private Integer C_ZIP;
    private String C_PHONE;
    private Timestamp C_SINCE;
    private String C_CREDIT;
    private Double C_CREDIT_LIM;
    private Double C_DISCOUNT;
    private Double C_BALANCE;
    private Double C_YTD_PAYMENT;
    private Integer C_PAYMENT_CNT;
    private Integer C_DELIVERY_CNT;
    private String C_DATA;


    public Customer() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public Customer( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public Customer( Integer c_ID, Integer c_D_ID, Integer c_W_ID, String c_LAST, String c_MIDDLE,
            String c_FIRST,
            String c_STREET_1, String c_STREET_2, String c_CITY, String c_STATE, Integer c_ZIP,
            String c_PHONE, Timestamp c_SINCE, String c_CREDIT, Double c_CREDIT_LIM, Double c_DISCOUNT,
            Double c_BALANCE, Double c_YTD_PAYMENT, Integer c_PAYMENT_CNT, Integer c_DELIVERY_CNT,
            String c_DATA ) {
        C_ID = c_ID;
        C_D_ID = c_D_ID;
        C_W_ID = c_W_ID;
        C_LAST = c_LAST;
        C_MIDDLE = c_MIDDLE;
        C_FIRST = c_FIRST;
        C_STREET_1 = c_STREET_1;
        C_STREET_2 = c_STREET_2;
        C_CITY = c_CITY;
        C_STATE = c_STATE;
        C_ZIP = c_ZIP;
        C_PHONE = c_PHONE;
        C_SINCE = c_SINCE;
        C_CREDIT = c_CREDIT;
        C_CREDIT_LIM = c_CREDIT_LIM;
        C_DISCOUNT = c_DISCOUNT;
        C_BALANCE = c_BALANCE;
        C_YTD_PAYMENT = c_YTD_PAYMENT;
        C_PAYMENT_CNT = c_PAYMENT_CNT;
        C_DELIVERY_CNT = c_DELIVERY_CNT;
        C_DATA = c_DATA;
    }


    public Double getC_CREDIT_LIM() {
        return C_CREDIT_LIM;
    }


    public void setC_CREDIT_LIM( Double c_CREDIT_LIM ) {
        C_CREDIT_LIM = c_CREDIT_LIM;
    }


    public Double getC_DISCOUNT() {
        return C_DISCOUNT;
    }


    public void setC_DISCOUNT( Double c_DISCOUNT ) {
        C_DISCOUNT = c_DISCOUNT;
    }


    public Double getC_BALANCE() {
        return C_BALANCE;
    }


    public void setC_BALANCE( Double c_BALANCE ) {
        C_BALANCE = c_BALANCE;
    }


    public Double getC_YTD_PAYMENT() {
        return C_YTD_PAYMENT;
    }


    public void setC_YTD_PAYMENT( Double c_YTD_PAYMENT ) {
        C_YTD_PAYMENT = c_YTD_PAYMENT;
    }


    public Integer getC_ID() {
        return C_ID;
    }


    public void setC_ID( Integer c_ID ) {
        C_ID = c_ID;
    }


    public Integer getC_D_ID() {
        return C_D_ID;
    }


    public void setC_D_ID( Integer c_D_ID ) {
        C_D_ID = c_D_ID;
    }


    public Integer getC_W_ID() {
        return C_W_ID;
    }


    public void setC_W_ID( Integer c_W_ID ) {
        C_W_ID = c_W_ID;
    }


    public String getC_LAST() {
        return C_LAST;
    }


    public void setC_LAST( String c_LAST ) {
        C_LAST = c_LAST;
    }


    public String getC_MIDDLE() {
        return C_MIDDLE;
    }


    public void setC_MIDDLE( String c_MIDDLE ) {
        C_MIDDLE = c_MIDDLE;
    }


    public String getC_FIRST() {
        return C_FIRST;
    }


    public void setC_FIRST( String c_FIRST ) {
        C_FIRST = c_FIRST;
    }


    public String getC_STREET_1() {
        return C_STREET_1;
    }


    public void setC_STREET_1( String c_STREET_1 ) {
        C_STREET_1 = c_STREET_1;
    }


    public String getC_STREET_2() {
        return C_STREET_2;
    }


    public void setC_STREET_2( String c_STREET_2 ) {
        C_STREET_2 = c_STREET_2;
    }


    public String getC_CITY() {
        return C_CITY;
    }


    public void setC_CITY( String c_CITY ) {
        C_CITY = c_CITY;
    }


    public String getC_STATE() {
        return C_STATE;
    }


    public void setC_STATE( String c_STATE ) {
        C_STATE = c_STATE;
    }


    public Integer getC_ZIP() {
        return C_ZIP;
    }


    public void setC_ZIP( Integer c_ZIP ) {
        C_ZIP = c_ZIP;
    }


    public String getC_PHONE() {
        return C_PHONE;
    }


    public void setC_PHONE( String c_PHONE ) {
        C_PHONE = c_PHONE;
    }


    public Timestamp getC_SINCE() {
        return C_SINCE;
    }


    public void setC_SINCE( Timestamp c_SINCE ) {
        C_SINCE = c_SINCE;
    }


    public String getC_CREDIT() {
        return C_CREDIT;
    }


    public void setC_CREDIT( String c_CREDIT ) {
        C_CREDIT = c_CREDIT;
    }


    public Integer getC_PAYMENT_CNT() {
        return C_PAYMENT_CNT;
    }


    public void setC_PAYMENT_CNT( Integer c_PAYMENT_CNT ) {
        C_PAYMENT_CNT = c_PAYMENT_CNT;
    }


    public Integer getC_DELIVERY_CNT() {
        return C_DELIVERY_CNT;
    }


    public void setC_DELIVERY_CNT( Integer c_DELIVERY_CNT ) {
        C_DELIVERY_CNT = c_DELIVERY_CNT;
    }


    public String getC_DATA() {
        return C_DATA;
    }


    public void setC_DATA( String c_DATA ) {
        C_DATA = c_DATA;
    }


    @Override
    public String toString() {
        return "Customer{" +
                "C_ID=" + C_ID +
                ", C_D_ID=" + C_D_ID +
                ", C_W_ID=" + C_W_ID +
                ", C_LAST=" + C_LAST +
                ", C_MIDDLE='" + C_MIDDLE + '\'' +
                ", C_FIRST='" + C_FIRST + '\'' +
                ", C_STREET_1='" + C_STREET_1 + '\'' +
                ", C_STREET_2='" + C_STREET_2 + '\'' +
                ", C_CITY='" + C_CITY + '\'' +
                ", C_STATE='" + C_STATE + '\'' +
                ", C_ZIP=" + C_ZIP +
                ", C_PHONE='" + C_PHONE + '\'' +
                ", C_SINCE=" + C_SINCE +
                ", C_CREDIT='" + C_CREDIT + '\'' +
                ", C_CREDIT_LIM=" + C_CREDIT_LIM +
                ", C_DISCOUNT=" + C_DISCOUNT +
                ", C_BALANCE=" + C_BALANCE +
                ", C_YTD_PAYMENT=" + C_YTD_PAYMENT +
                ", C_PAYMENT_CNT=" + C_PAYMENT_CNT +
                ", C_DELIVERY_CNT=" + C_DELIVERY_CNT +
                ", C_DATA='" + C_DATA + '\'' +
                '}';
    }
}
