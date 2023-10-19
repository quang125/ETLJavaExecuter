package com.example.querygenerate.faction.base;

import com.example.querygenerate.faction.interfaces.IContinuableAction;
import com.example.querygenerate.faction.interfaces.IEndAction;
import lombok.AllArgsConstructor;

/**
 * @author LeeHuyyHoangg
 */
@AllArgsConstructor
public abstract class BaseEndAction extends BaseFAction implements IEndAction {
    protected final IContinuableAction baseAction;

    @Override
    public Exception getException() {
        return baseAction.getException();
    }

    @Override
    public boolean isDone() {
        return baseAction.isDone();
    }

    @Override
    public boolean canRun() {
        return baseAction.canRun();
    }

    @Override
    public void run() {
        baseAction.run();
    }
}
