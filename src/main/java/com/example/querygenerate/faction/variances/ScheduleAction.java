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
public class ScheduleAction extends BaseEndAction {

    @Setter
    private Duration duration;
    private long invokableTime;

    private boolean cancel = false;

    public ScheduleAction(IContinuableAction runnable, Duration duration) {
        super(runnable);
        this.duration = duration;
        invokableTime = System.currentTimeMillis();
    }

    public ScheduleAction(Runnable runnable, Duration duration) {
        this(new UnitAction(runnable), duration);
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
        invokableTime = invokableTime + duration.toMillis();
        schedule();
        super.run();
    }

    @Override
    public void cancel() {
        cancel = true;
        super.cancel();
    }
}
