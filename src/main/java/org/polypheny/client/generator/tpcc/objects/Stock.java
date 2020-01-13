package org.polypheny.client.generator.tpcc.objects;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.tpcc.Utils;


/**
 * Internal representation of Stock information according to 4.3.3.1 <p>
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Stock {

    private static final Logger logger = LogManager.getLogger();
    private Integer S_I_ID;
    private Integer S_W_ID;
    private Integer S_QUANTITY;
    private String S_DIST_01;
    private String S_DIST_02;
    private String S_DIST_03;
    private String S_DIST_04;
    private String S_DIST_05;
    private String S_DIST_06;
    private String S_DIST_07;
    private String S_DIST_08;
    private String S_DIST_09;
    private String S_DIST_10;
    private Integer S_YTD;
    private Integer S_ORDER_CNT;
    private Integer S_REMOTE_CNT;
    private String S_DATA;


    public Stock() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public Stock( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public Stock( Integer s_i_id, Integer s_w_id, Integer s_quantity, String s_dist_01,
            String s_dist_02,
            String s_dist_03, String s_dist_04, String s_dist_05, String s_dist_06, String s_dist_07,
            String s_dist_08, String s_dist_09, String s_dist_10, Integer s_ytd, Integer s_order_cnt,
            Integer s_remote_cnt, String s_data ) {
        S_I_ID = s_i_id;
        S_W_ID = s_w_id;
        S_QUANTITY = s_quantity;
        S_DIST_01 = s_dist_01;
        S_DIST_02 = s_dist_02;
        S_DIST_03 = s_dist_03;
        S_DIST_04 = s_dist_04;
        S_DIST_05 = s_dist_05;
        S_DIST_06 = s_dist_06;
        S_DIST_07 = s_dist_07;
        S_DIST_08 = s_dist_08;
        S_DIST_09 = s_dist_09;
        S_DIST_10 = s_dist_10;
        S_YTD = s_ytd;
        S_ORDER_CNT = s_order_cnt;
        S_REMOTE_CNT = s_remote_cnt;
        S_DATA = s_data;
    }


    @Override
    public String toString() {
        return "Stock{" +
                "S_I_ID=" + S_I_ID +
                ", S_W_ID=" + S_W_ID +
                ", S_QUANTITY=" + S_QUANTITY +
                ", S_DIST_01='" + S_DIST_01 + '\'' +
                ", S_DIST_02='" + S_DIST_02 + '\'' +
                ", S_DIST_03='" + S_DIST_03 + '\'' +
                ", S_DIST_04='" + S_DIST_04 + '\'' +
                ", S_DIST_05='" + S_DIST_05 + '\'' +
                ", S_DIST_06='" + S_DIST_06 + '\'' +
                ", S_DIST_07='" + S_DIST_07 + '\'' +
                ", S_DIST_08='" + S_DIST_08 + '\'' +
                ", S_DIST_09='" + S_DIST_09 + '\'' +
                ", S_DIST_10='" + S_DIST_10 + '\'' +
                ", S_YTD=" + S_YTD +
                ", S_ORDER_CNT=" + S_ORDER_CNT +
                ", S_REMOTE_CNT=" + S_REMOTE_CNT +
                ", S_DATA='" + S_DATA + '\'' +
                '}';
    }


    public Integer getS_I_ID() {
        return S_I_ID;
    }


    public void setS_I_ID( Integer s_I_ID ) {
        S_I_ID = s_I_ID;
    }


    public Integer getS_W_ID() {
        return S_W_ID;
    }


    public void setS_W_ID( Integer s_W_ID ) {
        S_W_ID = s_W_ID;
    }


    public Integer getS_QUANTITY() {
        return S_QUANTITY;
    }


    public void setS_QUANTITY( Integer s_QUANTITY ) {
        S_QUANTITY = s_QUANTITY;
    }


    public String getS_DIST_01() {
        return S_DIST_01;
    }


    public void setS_DIST_01( String s_DIST_01 ) {
        S_DIST_01 = s_DIST_01;
    }


    public String getS_DIST_02() {
        return S_DIST_02;
    }


    public void setS_DIST_02( String s_DIST_02 ) {
        S_DIST_02 = s_DIST_02;
    }


    public String getS_DIST_03() {
        return S_DIST_03;
    }


    public void setS_DIST_03( String s_DIST_03 ) {
        S_DIST_03 = s_DIST_03;
    }


    public String getS_DIST_04() {
        return S_DIST_04;
    }


    public void setS_DIST_04( String s_DIST_04 ) {
        S_DIST_04 = s_DIST_04;
    }


    public String getS_DIST_05() {
        return S_DIST_05;
    }


    public void setS_DIST_05( String s_DIST_05 ) {
        S_DIST_05 = s_DIST_05;
    }


    public String getS_DIST_06() {
        return S_DIST_06;
    }


    public void setS_DIST_06( String s_DIST_06 ) {
        S_DIST_06 = s_DIST_06;
    }


    public String getS_DIST_07() {
        return S_DIST_07;
    }


    public void setS_DIST_07( String s_DIST_07 ) {
        S_DIST_07 = s_DIST_07;
    }


    public String getS_DIST_08() {
        return S_DIST_08;
    }


    public void setS_DIST_08( String s_DIST_08 ) {
        S_DIST_08 = s_DIST_08;
    }


    public String getS_DIST_09() {
        return S_DIST_09;
    }


    public void setS_DIST_09( String s_DIST_09 ) {
        S_DIST_09 = s_DIST_09;
    }


    public String getS_DIST_10() {
        return S_DIST_10;
    }


    public void setS_DIST_10( String s_DIST_10 ) {
        S_DIST_10 = s_DIST_10;
    }


    public Integer getS_YTD() {
        return S_YTD;
    }


    public void setS_YTD( Integer s_YTD ) {
        S_YTD = s_YTD;
    }


    public Integer getS_ORDER_CNT() {
        return S_ORDER_CNT;
    }


    public void setS_ORDER_CNT( Integer s_ORDER_CNT ) {
        S_ORDER_CNT = s_ORDER_CNT;
    }


    public Integer getS_REMOTE_CNT() {
        return S_REMOTE_CNT;
    }


    public void setS_REMOTE_CNT( Integer s_REMOTE_CNT ) {
        S_REMOTE_CNT = s_REMOTE_CNT;
    }


    public String getS_DATA() {
        return S_DATA;
    }


    public void setS_DATA( String s_DATA ) {
        S_DATA = s_DATA;
    }


    /**
     * @param D_ID district is for which you want to get S_DIST_xx
     * @return getS_DIST_(D_ID)
     */
    public String retrieveS_DISTforID( Integer D_ID ) {
        try {
            if ( D_ID < 10 ) {
                Method method = this.getClass().getMethod( "getS_DIST_0" + D_ID );
                return (String) method.invoke( this );
            } else {
                Method method = this.getClass().getMethod( "getS_DIST_" + D_ID );
                return (String) method.invoke( this );
            }
        } catch ( SecurityException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e ) {
            throw new IllegalArgumentException( "There is no method getS_DIST_" + D_ID );
        }
    }


    /**
     * @param D_ID district for which you want to change S_DIST_xx
     * @param S_DIST_xx the new S_DIST_xx
     */
    public void fixSDISTforID( Integer D_ID, String S_DIST_xx ) {
        try {

            Method method = this.getClass()
                    .getMethod( "setS_DIST_" + (D_ID < 10 ? "0" + D_ID : D_ID), String.class );
            method.invoke( this, S_DIST_xx );

        } catch ( SecurityException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e ) {
            throw new IllegalArgumentException(
                    "There is no method setS_DIST_" + (D_ID < 10 ? "0" + D_ID : D_ID) );
        }
    }
}