package com.example.querygenerate.faction.interfaces;

/**
 * @author LeeHuyyHoangg
 */
@SuppressWarnings("AlibabaClassNamingShouldBeCamel")
public interface IFAction extends Runnable
{
    /**
     * Has value id running the action throw Exception, otherwise return null
     * @return the Exception occurred while trying to run, if any
     */
    Exception getException();

    /**
     * Is true if the action is executed successfully, otherwise false
     * @return if the action is done executed
     */
    boolean isDone();

    /**
     * Call this and the function will arrange itself to be executed,
     */
    void schedule();

    /**
     * Stop the execution, if still able
     */
    void cancel();

    /**
     * Check the constraint of the action
     * @return should the action be invoked
     */
    boolean canRun();
}

