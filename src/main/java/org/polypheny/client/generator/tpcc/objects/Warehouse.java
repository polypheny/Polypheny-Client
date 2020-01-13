package org.polypheny.client.generator.tpcc.objects;


import java.sql.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.tpcc.Utils;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Warehouse {

    private static final Logger logger = LogManager.getLogger();
    private Integer W_ID;
    private String W_NAME;
    private String W_STREET_1;
    private String W_STREET_2;
    private String W_CITY;
    private String W_STATE;
    private Integer W_ZIP;
    private Double W_TAX;
    private Double W_YTD;


    public Warehouse() {

    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public Warehouse( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public Warehouse( Integer w_id, String w_name, String w_street_1, String w_street_2,
            String w_city,
            String w_state, Integer w_zip, Double w_tax, Double w_ytd ) {
        W_ID = w_id;
        W_NAME = w_name;
        W_STREET_1 = w_street_1;
        W_STREET_2 = w_street_2;
        W_CITY = w_city;
        W_STATE = w_state;
        W_ZIP = w_zip;
        W_TAX = w_tax;
        W_YTD = w_ytd;
    }


    @Override
    public String toString() {
        return "Warehouse{" +
                "W_ID=" + W_ID +
                ", W_NAME='" + W_NAME + '\'' +
                ", W_STREET_1='" + W_STREET_1 + '\'' +
                ", W_STREET_2='" + W_STREET_2 + '\'' +
                ", W_CITY='" + W_CITY + '\'' +
                ", W_STATE='" + W_STATE + '\'' +
                ", W_ZIP=" + W_ZIP +
                ", W_TAX=" + W_TAX +
                ", W_YTD=" + W_YTD +
                '}';
    }


    public Integer getW_ID() {
        return W_ID;
    }


    public void setW_ID( Integer w_ID ) {
        W_ID = w_ID;
    }


    public String getW_NAME() {
        return W_NAME;
    }


    public void setW_NAME( String w_NAME ) {
        W_NAME = w_NAME;
    }


    public String getW_STREET_1() {
        return W_STREET_1;
    }


    public void setW_STREET_1( String w_STREET_1 ) {
        W_STREET_1 = w_STREET_1;
    }


    public String getW_STREET_2() {
        return W_STREET_2;
    }


    public void setW_STREET_2( String w_STREET_2 ) {
        W_STREET_2 = w_STREET_2;
    }


    public String getW_CITY() {
        return W_CITY;
    }


    public void setW_CITY( String w_CITY ) {
        W_CITY = w_CITY;
    }


    public String getW_STATE() {
        return W_STATE;
    }


    public void setW_STATE( String w_STATE ) {
        W_STATE = w_STATE;
    }


    public Integer getW_ZIP() {
        return W_ZIP;
    }


    public void setW_ZIP( Integer w_ZIP ) {
        W_ZIP = w_ZIP;
    }


    public Double getW_TAX() {
        return W_TAX;
    }


    public void setW_TAX( Double w_TAX ) {
        W_TAX = w_TAX;
    }


    public Double getW_YTD() {
        return W_YTD;
    }


    public void setW_YTD( Double w_YTD ) {
        W_YTD = w_YTD;
    }
}
