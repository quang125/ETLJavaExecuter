package com.example.querygenerate.faction.base;


import com.example.querygenerate.faction.interfaces.IStartAction;

/**
 * @author LeeHuyyHoangg
 */
public abstract class BaseStartAction extends BaseFAction implements IStartAction {

    @Override
    public boolean canRun() {
        return true;
    }
}