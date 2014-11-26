package code.dws.experiment.goldstandard;

import java.io.File;

/**
 * Listener interested in {@link File} changes.
 * 
 * @author Pascal Essiembre
 */
interface FileChangeListener
{
    /**
     * Invoked when a file changes.
     * 
     * @param fileName name of changed file.
     */
    public void fileChanged(File file);
}
