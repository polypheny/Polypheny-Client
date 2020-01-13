package org.polypheny.client.db.tpcc.transactions.postgres;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.tpcc.PostgresTpccBenchmarker;
import org.polypheny.client.db.tpcc.transactions.PaymentTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.Warehouse;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PostgresPaymentTransactionExecutor extends PaymentTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;
    private PostgresTpccBenchmarker benchmarker;


    public PostgresPaymentTransactionExecutor( DBConnector connector,
            PostgresTpccBenchmarker benchmarker ) {
        super( connector, benchmarker );
        this.connector = connector;
        this.benchmarker = benchmarker;
    }


    @Override
    protected Customer handleBCforCustomer( Customer customer, int d_id, int w_id, Double h_amount ) {
        String query = "SELECT C_DATA FROM tpcc_customer WHERE C_W_ID=" + customer.getC_W_ID() + " AND C_D_ID=" + customer.getC_D_ID() + " AND C_ID=" + customer.getC_ID();

        return executeQuery( resultSet -> {
            String C_DATA = resultSet.getString( "C_DATA" );
            String historyInfo = "" + customer.getC_ID() + customer.getC_D_ID() + customer.getC_W_ID() + d_id + w_id + h_amount;
            int oldlen = C_DATA.length();
            C_DATA = C_DATA.substring( 0, C_DATA.length() - historyInfo.length() ) + historyInfo;
            int newlen = C_DATA.length();
            if ( oldlen != newlen ) {
                logger.error( "old length {}, new length {}", oldlen, newlen );
            }
            String update = "UPDATE tpcc_customer SET C_DATA='" + C_DATA + "' WHERE C_W_ID=" + customer.getC_W_ID() + " AND C_D_ID=" + customer.getC_D_ID() + " AND C_ID=" + customer.getC_ID();
            executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );
            return new Customer( resultSet );
        }, query, QueryType.QUERYTYPESELECT );
    }


    @Override
    protected Customer getCustomerAndPay( int c_w_id, int c_d_id, Integer c_id, Double h_amount ) {
        String query = "update tpcc_customer SET C_BALANCE=c_balance-" + h_amount + ", c_ytd_payment=c_ytd_payment+" + h_amount + ",c_payment_cnt=c_payment_cnt+1 where c_id=" + c_id + " and c_d_id=" + c_d_id + " and c_w_id=" + c_w_id
                + " RETURNING C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE;";
        return executeQuery( Customer::new, query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected Customer getCustomerAndPay( int c_w_id, int c_d_id, String C_LAST, Double h_amount ) {
        String query =
                "UPDATE tpcc_customer SET c_balance   = c_balance - " + h_amount + ", c_ytd_payment = c_ytd_payment + " + h_amount + ", c_payment_cnt = c_payment_cnt + 1 WHERE c_last = '" + C_LAST + "' AND c_id = ( SELECT c_id FROM tpcc_customer WHERE c_last = '" + C_LAST + "' AND c_w_id=" + c_w_id
                        + " AND c_d_id=" + c_d_id + " ORDER BY c_first ASC LIMIT 1 OFFSET (SELECT ceil(count(*) :: NUMERIC / 2)-1 FROM tpcc_customer WHERE c_last = '" + C_LAST + "' AND c_d_id = " + c_d_id + " AND c_w_id = " + c_w_id + ") ) AND c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id
                        + " RETURNING *;";
        return executeQuery( Customer::new, query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected District getDistrictAndIncYTD( int d_w_id, int d_id, Double h_amount ) {
        String query = "update tpcc_district set d_ytd=d_ytd+" + h_amount + " WHERE d_w_id=" + d_w_id + " and d_id=" + d_id + " RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip";
        return executeQuery( District::new, query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected Warehouse getWarehouseAndIncYTD( int W_ID, Double H_AMOUNT ) {
        String query = "UPDATE tpcc_warehouse SET w_ytd=w_ytd+" + H_AMOUNT + " WHERE w_id=" + W_ID
                + " RETURNING w_name, w_street_1,  w_street_2, w_city, w_state, w_zip";
        return executeQuery( Warehouse::new, query, QueryType.QUERYTYPEUPDATE );
    }
}
