package com.example.querygenerate.faction.variances;

import com.example.querygenerate.faction.base.BaseChainAction;
import com.example.querygenerate.faction.interfaces.IContinuableAction;
import lombok.Getter;

import java.time.Duration;

/**
 * @author LeeHuyyHoangg
 */
@Getter
public class DelayAction extends BaseChainAction {

    private final long createTime = System.currentTimeMillis();
    private final Duration delayTime;

    public DelayAction(IContinuableAction continuableAction, Duration delayTime) {
        super(continuableAction);
        this.delayTime = delayTime;
    }

    public DelayAction(Runnable continuableAction, Duration delayTime) {
        this(new UnitAction(continuableAction), delayTime);
    }

    @Override
    public boolean canRun() {
        return createTime + delayTime.toMillis() < System.currentTimeMillis();
    }
}
