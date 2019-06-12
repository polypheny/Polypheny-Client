package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects;


import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.Utils;
import java.sql.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * District according to 4.3.3.1 <p>
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class District {

    private static final Logger logger = LogManager.getLogger();

    private Integer D_ID;
    private Integer D_W_ID;
    private String D_NAME;
    private String D_STREET_1;
    private String D_STREET_2;
    private String D_CITY;
    private String D_STATE;
    private Integer D_ZIP;
    private Double D_TAX;
    private Double D_YTD;
    private Integer D_NEXT_O_ID;


    public District() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public District( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public District( Integer d_ID, Integer d_W_ID, String d_NAME, String d_STREET_1, String d_STREET_2, String d_CITY, String d_STATE, Integer d_ZIP, Double d_TAX, Double d_YTD, Integer d_NEXT_O_ID ) {
        D_ID = d_ID;
        D_W_ID = d_W_ID;
        D_NAME = d_NAME;
        D_STREET_1 = d_STREET_1;
        D_STREET_2 = d_STREET_2;
        D_CITY = d_CITY;
        D_STATE = d_STATE;
        D_ZIP = d_ZIP;
        D_TAX = d_TAX;
        D_YTD = d_YTD;
        D_NEXT_O_ID = d_NEXT_O_ID;
    }


    public Integer getD_ID() {
        return D_ID;
    }


    public void setD_ID( Integer d_ID ) {
        D_ID = d_ID;
    }


    public Integer getD_W_ID() {
        return D_W_ID;
    }


    public void setD_W_ID( Integer d_W_ID ) {
        D_W_ID = d_W_ID;
    }


    public String getD_NAME() {
        return D_NAME;
    }


    public void setD_NAME( String d_NAME ) {
        D_NAME = d_NAME;
    }


    public String getD_STREET_1() {
        return D_STREET_1;
    }


    public void setD_STREET_1( String d_STREET_1 ) {
        D_STREET_1 = d_STREET_1;
    }


    public String getD_STREET_2() {
        return D_STREET_2;
    }


    public void setD_STREET_2( String d_STREET_2 ) {
        D_STREET_2 = d_STREET_2;
    }


    public String getD_CITY() {
        return D_CITY;
    }


    public void setD_CITY( String d_CITY ) {
        D_CITY = d_CITY;
    }


    public String getD_STATE() {
        return D_STATE;
    }


    public void setD_STATE( String d_STATE ) {
        D_STATE = d_STATE;
    }


    public Integer getD_ZIP() {
        return D_ZIP;
    }


    public void setD_ZIP( Integer d_ZIP ) {
        D_ZIP = d_ZIP;
    }


    public Double getD_TAX() {
        return D_TAX;
    }


    public void setD_TAX( Double d_TAX ) {
        D_TAX = d_TAX;
    }


    public Double getD_YTD() {
        return D_YTD;
    }


    public void setD_YTD( Double d_YTD ) {
        D_YTD = d_YTD;
    }


    public Integer getD_NEXT_O_ID() {
        return D_NEXT_O_ID;
    }


    public void setD_NEXT_O_ID( Integer d_NEXT_O_ID ) {
        D_NEXT_O_ID = d_NEXT_O_ID;
    }


    @Override
    public String toString() {
        return "District{" +
                "D_ID=" + D_ID +
                ", D_W_ID=" + D_W_ID +
                ", D_NAME='" + D_NAME + '\'' +
                ", D_STREET_1='" + D_STREET_1 + '\'' +
                ", D_STREET_2='" + D_STREET_2 + '\'' +
                ", D_CITY='" + D_CITY + '\'' +
                ", D_STATE='" + D_STATE + '\'' +
                ", D_ZIP=" + D_ZIP +
                ", D_TAX=" + D_TAX +
                ", D_YTD=" + D_YTD +
                ", D_NEXT_O_ID=" + D_NEXT_O_ID +
                '}';
    }
}
