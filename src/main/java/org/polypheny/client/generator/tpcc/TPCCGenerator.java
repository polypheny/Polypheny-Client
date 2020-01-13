package org.polypheny.client.generator.tpcc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.RandomGenerator;


/**
 * Generates Random Numbers as specified by TPC-C <p>
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class TPCCGenerator {

    private static int C_LOAD = -1;
    private static int C_RUN = -1;
    private static int C_DELTA = -1;

    private static Logger logger = LogManager.getLogger();


    /**
     * 2.1.6.1 Let C-Load be the value of C used to generate C_LAST when populating the database. C-Load is a value in the range of [0..255] including 0 and 255.
     */
    public static int getcLoad() {
        if ( C_LOAD == -1 ) {
            generateC();
        }
        return C_LOAD;
    }


    /**
     * 2.1.6.1 Let C-Run be the value of C used to generate C_LAST for the measurement run.
     */
    public static int getcRun() {
        logger.trace( "Accessing C-Run. This should only happen on the server" );
        if ( C_RUN == -1 ) {
            generateC();
        }
        return C_RUN;
    }


    /**
     * 2.1.6.1 Let C-Delta be the absolute value of the difference between C -Load and C-Run. C-Delta must be a value in the range of [65..119] inclu ding the values of 65 and 119 and excluding the value of 96 and 112.
     */
    static int getcDelta() {
        if ( C_DELTA == -1 ) {
            generateC();
        }
        return C_DELTA;
    }


    /**
     * 2.1.6 For C_LAST, the range is [0 .. 999] and A = 255
     *
     * @param CC_Last C is a run-time constant randomly chosen within [0 .. A] that can be varied without altering performance. The same C value, per field (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals. Use either {{@link #getcLoad()}} or {@link #getcRun()} depending on which
     * stage of the program you are in.
     */
    public static int getCLast( int CC_Last ) {
        return NURand( 255, 0, 999, CC_Last );
    }


    /**
     * 2.1.6 For C_ID, the range is [1 .. 3000] and A = 1023
     *
     * @param CC_ID see documentation of the parameter {@link #getCLast(int)}
     */
    public static int getCID( int CC_ID ) {
        return NURand( 1023, 1, 3000, CC_ID );
    }


    /**
     * 2.1.6 For OL_I_ID, the range is [1 .. 100000] and A = 8191
     *
     * @param CCOL_I_ID see documentation {@link #getCLast(int)}
     */
    public static int getCOLIID( int CCOL_I_ID ) {
        return NURand( 8191, 1, 100000, CCOL_I_ID );
    }


    /**
     * 2.1.6 NURand (A, x, y) = (((random (0, A) | random (x, y)) + C) % (y - x + 1)) + x exp -1 | exp -2 stands for the bitwise logical OR operation between exp -1 and exp -2 exp -1 % exp -2 stands for exp -1 modulo exp -2
     *
     * @param A A is a constant chosen according to the size of the range [x .. y] for C_LAST, the range is [0 .. 999] and A = 255 for C_ID, the range is [1 .. 3000] and A = 1023 for OL_I_ID the range is [1 .. 100000] and A = 8191
     * @param x inclusive lower bound
     * @param y inclusive upper bound
     * @param C If you are reading the documentation of this parameter, you shouldn't be calling this function. {@link #getCLast(int)}
     */
    public static int NURand( int A, int x, int y, int C ) {
        return (((RandomGenerator.generateUniform( 0, A ) | RandomGenerator.generateUniform( x, y ))
                + C) % (y - x + 1)) + x;
    }


    /**
     * Re-generates Cs
     */
    public static void generateC() {
        if ( C_LOAD != -1 ) {
            throw new IllegalStateException(
                    "You should not be calling generateC() twice. It will be called automatically anyway if a C has not been initiated." );
        }
        int cload, crun;
        do {//See Page 21, Section 2.1
            cload = RandomGenerator.generateUniform( 0, 255 );
            crun = RandomGenerator.generateUniform( 0, 255 );
            //The following condition is not a joke. The TPC-C Specification actually declares 96 and 112 as invalid numbers
        } while ( Math.abs( cload - crun ) < 65 || Math.abs( cload - crun ) > 119
                || Math.abs( cload - crun ) == 96 || Math.abs( cload - crun ) == 112 );
        C_LOAD = cload;
        C_RUN = crun;
        C_DELTA = Math.abs( cload - crun );
        logger.trace( "Generated valid C_RUN {}, C_LOAD {} and C_DELTA {}", C_RUN, C_LOAD, C_DELTA );
    }
}
