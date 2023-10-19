package com.example.querygenerate.faction.variances;

import com.example.querygenerate.faction.base.BaseStartAction;
import lombok.Getter;

/**
 * @author LeeHuyyHoangg
 */
@Getter
public class UnitAction extends BaseStartAction {

    private final Runnable runnable;
    private Exception exception;
    private boolean done;

    public UnitAction(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try {
            runnable.run();
            done = true;
        } catch (Exception e) {
            exception = e;
        }
    }
}
