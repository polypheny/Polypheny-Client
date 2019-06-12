package ch.unibas.dmi.dbis.polyphenydb.client.generator;


import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiManager;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Provides support for various Random Distributions / Generation as specified in the TPC-C Benchmark.
 *
 * @author Silvan Heller
 */
public class RandomGenerator {

    private static final Random rand = new Random();
    private static Logger logger = LogManager.getLogger();
    /*
     * We generate only the emoji string as fillers.
     * This is because we don't have a safe SQL-Escaping mechanism for random strings yet.
     * This tests UTF-8 capability */
    private static String charset = "";


    static {
        StringBuilder sb = new StringBuilder();
        int cnt = 0;
        for ( Emoji emoji : EmojiManager.getAll() ) {
            //Don't use emojis with length > 1 because we can only generate chars.
            if ( emoji.getUnicode().length() > 1 ) {
                continue;
            }
            sb.append( emoji.getUnicode().toCharArray()[0] );
            cnt++;
            if ( cnt > 128 - 62 ) { //26*2+10=62, tpcc needs at most 128 chars
                break;
            }
        }
        sb.append( "abcdefghijklmnopqrstuvwxyz" );
        sb.append( "abcdefghijklmnopqrstuvwxyz".toUpperCase() );
        sb.append( "0123456789" );
        charset = sb.toString();
    }


    /**
     * Generates a uniformly distributed random number between lb and ub
     *
     * @param lb lower bound (inclusive)
     * @param ub upper bound (inclusive)
     */
    public static int generateUniform( int lb, int ub ) {
        return new UniformIntegerDistribution( lb, ub ).sample();
    }


    /**
     * 4.3.2.2 TPC-C The notation random a-string [x .. y ] represents a string of random alphanumeric characters of a random length of minimum x, maximum y, and mean (y+x)/ 2. Comment: The character set used must be able to rep resent a minimum of 128 different characters. The character set used
     * must include at least 26 lower case letters, 26 upper case letters, and the digits „ 0‟ to „ 9‟ .
     *
     * @param x minimum String length (inclusive)
     * @param y maximum String length (inclusive)
     */
    public static String generateAString( int x, int y ) {
        int length = generateUniform( x, y );
        return RandomStringUtils.random( length, charset );
    }


    /**
     * Generates a string of random numeric characters of a random length of minimum x, maximum y and mean (y+x)/2 characters. See TPC-C Documentation 4.3.2.2
     *
     * @param x minimum String length (inclusive)
     * @param y maximum String length (inclusive)
     */
    public static String generateNString( int x, int y ) {
        int length = generateUniform( x, y );
        return RandomStringUtils.randomNumeric( length );
    }
}
