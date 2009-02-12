package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * FIXME False start. Need a page that will hold positions, with a flag
 * indicating that it is a temporary address or a vacuum journal. Also, need a
 * page that will track positions. Expect that the page will be lazy. There will
 * be a count for each alignment. That will be maintained carefully. There will
 * be bags of pages, and a pointer to the bags, that will be maintained
 * sloppily. Pages will be written to the bag, but they won't be removed unless
 * searching for a particular size. That is three new page types.
 * 
 * @author Alan Gutierrez
 */
public class VacuumNodePool
{
    private final SortedSet<Long> vacuumNodes;
    
    public VacuumNodePool()
    {
        this.vacuumNodes = new TreeSet<Long>();
    }
    
    public long getVacuumNode()
    {
        long vacuumNode = 0L;
        synchronized (vacuumNodes)
        {
            vacuumNode = vacuumNodes.first();
            vacuumNodes.remove(vacuumNode);
        }
        return vacuumNode;
    }

    public long getVacuumNode(MutatorFactory mutatorFactory, Header header)
    {
        long vacuumNode = 0L;
        synchronized (vacuumNodes)
        {
            while (vacuumNode == 0L)
            {
                if (vacuumNodes.size() == 0)
                {
                    try
                    {
                        vacuumNodes.wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
                else if (vacuumNodes.size() == 1)
                {
                    ByteBuffer destination = ByteBuffer.allocate(VacuumNode.SIZE);
                    Mutator mutator = mutatorFactory.mutate();
                    vacuumNodes.add(mutator.allocate(VacuumNode.SIZE));
                    vacuumNodes.add(mutator.allocate(VacuumNode.SIZE));
                    for (long address : vacuumNodes)
                    {
                        VacuumNode node = new VacuumNode();
    
                        node.setAddress(0L);
                        node.setNextVacuumNode(header.getFirstVacuumNode());
                        
                        node.write(destination);
                        destination.flip();
                        
                        mutator.write(address, destination);
                        
                        destination.clear();
                    }
                    
                    mutator.commitVacuumNodes();
                }
                else
                {
                    vacuumNode = vacuumNodes.first();
                    vacuumNodes.remove(vacuumNode);
                }
            }
        }
        return vacuumNode;
    }
}
