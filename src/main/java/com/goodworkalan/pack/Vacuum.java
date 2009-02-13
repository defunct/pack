package com.goodworkalan.pack;

import java.util.Set;

public interface Vacuum
{
    public void vacuum(Mover mover, ByRemainingTable byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages);
}
