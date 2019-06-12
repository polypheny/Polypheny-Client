package ch.unibas.dmi.dbis.polyphenydb.client.main;


import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;


abstract class AbstractCommand implements Runnable {

    @Option(title = "Verbosity", name = { "-v", "--verbose" }, description = "Enables verbosity.", type = OptionType.GLOBAL)
    protected boolean verbose = false;


    @Override
    public abstract void run();
}
