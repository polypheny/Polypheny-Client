package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.generateWarehouse;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Stock;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class StockTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void getS_DISTforIDTest() {
        Stock stock = new Stock( -1, -1, -1, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", -1, -1,
                -1, "" ); //01 to 10 are just the numbers
        for ( int i = 1; i <= 10; i++ ) {
            String s = stock.retrieveS_DISTforID( i );
            if ( Integer.parseInt( s ) != i ) {
                logger.error( "Fetched String is {} and should have been {}", s, i );
            }
        }
    }


    @Test
    public void setSDISTforIDTest() {
        Stock stock = new Stock();
        for ( int i = 1; i <= 10; i++ ) {
            String s = String.valueOf( i );
            stock.fixSDISTforID( i, s );
            if ( !stock.retrieveS_DISTforID( i ).equals( s ) ) {
                fail();
            }
        }
    }


    @Test
    public void generateStockForWarehouse() {
        Warehouse warehouse = generateWarehouse();
        Stock[] stocks = TPCCPopulationGenerator.generateStockForWarehouse( warehouse );
        HashSet<Integer> ids = new HashSet<>();
        int containsOriginal = 0;
        for ( Stock stock : stocks ) {
            ids.add( stock.getS_I_ID() );
            verifyStock( stock, warehouse );
            if ( stock.getS_DATA().contains( "ORIGINAL" ) ) {
                containsOriginal++;
            }
        }
        if ( ids.size() != 100000 ) {
            logger.error( "{} unique ids were generated instead of {}", ids.size(), 100000 );
            fail();
        }
        if ( containsOriginal == stocks.length / 10 ) {
            logger.warn(
                    "ORIGINAL was in precisely 10% of the generated strings. It was in {} of {} strings",
                    containsOriginal, stocks.length );
        }
        /**
         * According to the TPC-C Specifications at the bottom of page 68, 5% Variation of target cardinality is allowed.
         */
        if ( Math.abs( containsOriginal - stocks.length / 10 ) > stocks.length / 100 * 5 ) {
            logger.error( "original was in {} strings, stock size was {}", containsOriginal,
                    stocks.length );
            fail();
        }
    }


    /**
     * Verifies a given {@link Stock} if it is valid and belongs to the given {@link Warehouse}
     */
    private void verifyStock( Stock stock, Warehouse warehouse ) {
        if ( stock.getS_W_ID() != warehouse.getW_ID() ) {
            logger.error( "S_W_ID was {} instead of {}", stock.getS_W_ID(), warehouse.getW_ID() );
            fail();
        }
        if ( stock.getS_QUANTITY() < 10 || stock.getS_QUANTITY() > 100 ) {
            logger.error( "S_QUANTITY was {}", stock.getS_QUANTITY() );
            fail();
        }
        checkStringLength( stock.getS_DIST_01(), 24, 24 );
        checkStringLength( stock.getS_DIST_02(), 24, 24 );
        checkStringLength( stock.getS_DIST_03(), 24, 24 );
        checkStringLength( stock.getS_DIST_04(), 24, 24 );
        checkStringLength( stock.getS_DIST_05(), 24, 24 );
        checkStringLength( stock.getS_DIST_06(), 24, 24 );
        checkStringLength( stock.getS_DIST_07(), 24, 24 );
        checkStringLength( stock.getS_DIST_08(), 24, 24 );
        checkStringLength( stock.getS_DIST_09(), 24, 24 );
        checkStringLength( stock.getS_DIST_10(), 24, 24 );
        if ( stock.getS_YTD() != 0 ) {
            logger.error( "S_YTD was {}", stock.getS_YTD() );
            fail();
        }
        if ( stock.getS_ORDER_CNT() != 0 ) {
            logger.error( "S_ORDER_CNT was {}", stock.getS_ORDER_CNT() );
            fail();
        }
        if ( stock.getS_REMOTE_CNT() != 0 ) {
            logger.error( "S_REMOTE_CNT was {}", stock.getS_REMOTE_CNT() );
            fail();
        }
        checkStringLength( stock.getS_DATA(), 26, 50 );
    }
}
