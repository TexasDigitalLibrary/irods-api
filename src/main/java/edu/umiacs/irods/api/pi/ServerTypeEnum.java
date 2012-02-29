/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umiacs.irods.api.pi;

/**
 * Values for serverType in MiscSvrInfo_PI
 * 
 * #define RCAT_NOT_ENABLED        0
 * #define RCAT_ENABLED            1
 * @author toaster
 */
public enum ServerTypeEnum {

    RCAT_NOT_ENABLED(0), RCAT_ENABLED(1);
    
    private int i;
    
    ServerTypeEnum(int i)
    {
        this.i = i;
    }
    
    public static ServerTypeEnum valueOf(int i)
    {
        switch(i)
        {
            case 0:
                return RCAT_NOT_ENABLED;
            case 1:
                return RCAT_ENABLED;
            default:
                throw new IllegalArgumentException("No server type for value: " + i);
        }
    }
}
