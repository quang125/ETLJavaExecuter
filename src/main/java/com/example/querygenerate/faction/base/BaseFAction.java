package com.example.querygenerate.faction.base;

import com.example.querygenerate.faction.interfaces.IFAction;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author LeeHuyyHoangg
 */
@SuppressWarnings("AlibabaClassNamingShouldBeCamel")
@Getter
public abstract class BaseFAction implements IFAction {
    protected static final LinkedBlockingQueue<BaseFAction> ACTION_QUEUE = new LinkedBlockingQueue<>();

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    static {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            List<BaseFAction> actionWraps = new ArrayList<>();
            synchronized (ACTION_QUEUE) {
                ACTION_QUEUE.drainTo(actionWraps);
            }
            for (BaseFAction actionWrap : actionWraps) {
                if (actionWrap.canRun()) {
                    EXECUTOR.execute(actionWrap);
                } else {
                    ACTION_QUEUE.add(actionWrap);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void schedule() {
        ACTION_QUEUE.add(this);
    }

    public void cancel() {
        ACTION_QUEUE.remove(this);
    }
}
