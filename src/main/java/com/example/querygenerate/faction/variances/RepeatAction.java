package com.example.querygenerate.faction.variances;

import com.example.querygenerate.faction.base.BaseEndAction;
import com.example.querygenerate.faction.interfaces.IContinuableAction;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

/**
 * @author LeeHuyyHoangg
 */
@Getter
public class RepeatAction extends BaseEndAction {

    @Setter
    private Duration duration;
    private long invokableTime;

    private boolean cancel = false;

    public RepeatAction(IContinuableAction continuableAction, Duration duration) {
        super(continuableAction);
        this.duration = duration;
        invokableTime = System.currentTimeMillis();
    }

    public RepeatAction(Runnable continuableAction, Duration duration) {
        this(new UnitAction(continuableAction), duration);
    }

    @Override
    public boolean canRun() {
        return invokableTime < System.currentTimeMillis();
    }

    @Override
    public void run() {
        if (cancel) {
            return;
        }
        super.run();
        invokableTime = System.currentTimeMillis() + duration.toMillis();
        schedule();
    }

    @Override
    public void cancel() {
        cancel = true;
        super.cancel();
    }
}