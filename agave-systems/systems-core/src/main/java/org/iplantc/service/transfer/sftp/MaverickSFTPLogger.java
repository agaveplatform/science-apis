package org.iplantc.service.transfer.sftp;

import org.apache.log4j.Logger;
import org.iplantc.service.transfer.Settings;

import com.sshtools.logging.LoggerLevel;

public class MaverickSFTPLogger 
 implements com.sshtools.logging.Logger
{
    // Constants.
    private static final Logger _log = Logger.getLogger(MaverickSFTPLogger.class);
    private static final LoggerLevel DEFAULT_LOG_LEVEL = LoggerLevel.ERROR;
    
    // Fields.
    private static MaverickSFTPLogger _instance; // singleton
    private LoggerLevel _level;
    
    // Get singleton instance.
    public static MaverickSFTPLogger getInstance()
    {
        // See if the singleton instance has been created.
        if (_instance == null) 
        {
            // Only synchronize when on initialization.
            synchronized (MaverickSFTPLogger.class)
            {
                // No race condition inside the sync block,
                // so we test singleton existence again.
                if (_instance == null) 
                {
                    // Try to assign the logging level from the settings file.
                    LoggerLevel level = DEFAULT_LOG_LEVEL;
                    try {level = LoggerLevel.valueOf(Settings.MAVERICK_LOG_LEVEL);}
                        catch (Exception e) {
                            // Log the error and continue using the default value.
                            String msg = "Unable to set Maverick log level using Settings.MAVERICK_LOG_LEVEL." +
                                         " Using default value " + DEFAULT_LOG_LEVEL.name() + ": " + e.getMessage();
                            _log.error(msg, e);
                        }
                    
                    // Create the singleton instance.
                    _instance = new MaverickSFTPLogger(level);
                }
            }
        }
        return _instance;
    }

    // Constructor.
    private MaverickSFTPLogger(LoggerLevel level)
    {
        _level = level;
        _log.info("Created MaverickSFTPLogger with logging level " + _level.name() + ".");
    }
    
    @Override
    public boolean isLevelEnabled(LoggerLevel level) {
        return _level.ordinal() >= level.ordinal();
    }

    @Override
    public void log(LoggerLevel level, Object source, String msg) {
        // Check the level even though this does not 
        // appear to be the Maverick convention.
        if (_level.ordinal() < level.ordinal()) return;
        
        // Write to the actual logger.
        if (level == LoggerLevel.DEBUG) _log.debug(msg);
        else if (level == LoggerLevel.INFO) _log.info(msg);
        else if (level == LoggerLevel.ERROR) _log.error(msg);
    }

    @Override
    public void log(LoggerLevel level, Object source, String msg, Throwable t) {
        // Check the level even though this does not 
        // appear to be the Maverick convention.
        if (_level.ordinal() < level.ordinal()) return;
        
        // Write to the actual logger.
        if (level == LoggerLevel.DEBUG) _log.debug(msg, t);
        else if (level == LoggerLevel.INFO) _log.info(msg, t);
        else if (level == LoggerLevel.ERROR) _log.error(msg, t);
    }
}
