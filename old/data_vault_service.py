#!/usr/bin/env python

import logging
from data_vault   import DataVault
from return_value import ReturnValue

"""
Implements the Data Vault client interface.
"""

logger_data = logging.getLogger("DataAccess")
logger_data.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s [%(message)s", datefmt="%Y-%m-%d @ %H:%M:%S")
ch.setFormatter(fmt)
logger_data.addHandler(ch)

vaults = {}

def create_vault(vaultID, uid):
    """
    Create a new Data Vault.

    @param vaultID -- name of the new Data Vault
    @uid           -- ID of the user making the request
    """
    logmsg = "'%s' on vault '%s'] create vault" % (uid, vaultID)
    try:
        retval = _authorize_request("create_vault", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)

        vaults[vaultID] = DataVault(vaultID)
        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)


def delete_vault(vaultID, uid):
    """
    Delete an existing Data Vault.
    All data stored inside the vault is lost.

    @param vaultID -- name of the Data Vault to be deleted
    @uid           -- ID of the user making the request
    """  
    logmsg = "'%s' on vault '%s'] delete vault" % (uid, vaultID)
    try:
        retval = _authorize_request("delete_vault", uid)            
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)

        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        del vaults[vaultID]
        
        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)


def create_data_type_scalar(DTname, DTbase_name, DTCname, DTCvalue, vaultID, uid):
    """
    Create a new scalar Data Type.

    @param DTname      -- name of the new scalar Data Type
    @param DTbase_name -- name of the built-in scalar Data Type on which the new Data Type is based
    @param DTCname     -- name of the Data Type Constraint attached to the new Data Type
    @param vaultID     -- name of the Data Vault to which the new Data Type is being added
    @param uid         -- ID of the user making the request
    """
    s = ""
    for i in range(len(DTCname)):
        s += (" and %s(%s)" if i > 0 else "%s(%s)") % (DTCname[i], DTCvalue[i])    
    logmsg = "'%s' on vault '%s'] create scalar data type '%s': %s with constraints %s" % (uid, vaultID, DTname, DTbase_name, s)
    try:
        retval = _authorize_request("create_data_type_scalar", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)
        
        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        vault = vaults[vaultID]
        vault.create_data_type_scalar(DTname, DTbase_name, DTCname, DTCvalue)

        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)


def create_data_type_composite(DTname, datafields, vaultID, uid):
    """
    Create a new composite Data Type.

    @param DTname     -- name of the new composite Data Type
    @param datafields -- a (nested) dictionary storing the names of the data fields (as key) and their Data Type (as value)
    @param vaultID    -- name of the Data Vault to which the new Data Type is being added
    @param uid        -- ID of the user making the request
    """
    logmsg = "'%s' on vault '%s'] create composite data type '%s': %s" % (uid, vaultID, DTname, datafields)
    try:
        retval = _authorize_request("create_data_type_composite", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)
        
        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        vault = vaults[vaultID]
        vault.create_data_type_composite(DTname, datafields)

        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)


def create_data_item(DIname, DTname, vaultID, uid):
    """
    Create a new Data Item.
    
    @param DIname  -- name of the new Data Item being created
    @param DTname  -- name of the Data Type associated with the new Data Item.
    @param vaultID -- name of the Data Vault to which the new Data Item is being added
    @param uid     -- ID of the user making the request
    """
    logmsg = "'%s' on vault '%s'] create data item '%s' of type '%s'" % (uid, vaultID, DIname, DTname)
    try:
        retval = _authorize_request("create_data_item", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)
        
        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        vault = vaults[vaultID]
        vault.create_data_item(DIname, DTname)

        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)
    

def create_incarnation(DIname, key, value, vaultID, uid):
    """
    Create a new incarnation (managed instance) of a Data Item.
    
    @param DIname -- the Data Item name
    @param key    -- unique (among incarnations of the same Data Item) incarnation ID 
    @param value  -- the initial value of the incarnation
    @param vaultID  -- name of the Data Vault to which the new incarnation is being added
    @param uid    -- ID of the user making the request
    """
    logmsg = "'%s' on vault '%s'] create incarnation of '%s' with key='%s'  & value='%s'" % (uid, vaultID, DIname, key, value)
    try:
        retval = _authorize_request("create_incarnation", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)
        
        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        vault = vaults[vaultID]
        vault.create_data_item_incarnation(DIname, key, value)

        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)
    

def update_incarnation(DIname, key, value, vaultID, uid):
    """
    Updates the value of an existing Data Item incarnation.
    
    @param DIname  -- the Data Item name
    @param key     -- unique (among incarnations of the same Data Item) incarnation ID
    @param value   -- the updated value of the incarnation
    @param vaultID -- name of the Data Vault storing the incarnation
    @param uid     -- ID of the user making the request    
    """
    logmsg = "'%s' on vault '%s'] update incarnation of '%s' with key='%s' to value='%s'" % (uid, vaultID, DIname, key, value)
    try:
        retval = _authorize_request("update_incarnation", uid)
        if not retval.ok:
            logger_data.info(logmsg + " [FAILED: can't authorize, %s]" % retval.errmsg)
            return ReturnValue(None, False, retval.errmsg)
        
        if not vaults.has_key(vaultID):
            raise Exception("vault '%s' not found" % vaultID)

        vault = vaults[vaultID]
        vault.update_incarnation(DIname, key, value)

        logger_data.info(logmsg + " [OK]")
    except Exception as err:
        logger_data.info(logmsg + " [FAILED: %s]" % err)


def _authorize_request(action, uid):
    """
    Check whether a given user can perform a given action.

    @param action -- the name of the action the user would like to perform
    @param uid    -- ID of the user being authenticated
    @return 
    """
    actions = ["create_vault", "delete_vault",
               "create_data_type_scalar", "create_data_type_composite",
               "create_data_item",
               "create_incarnation", "update_incarnation"]
    try:
        if action not in actions:
            return ReturnValue(None, False, "unknown request type '%s'" % action)
        else:
            return ReturnValue(None, True)
    except:
        pass
        
    

if __name__ == "__main__":
    create_vault              ("OPTAES", "cciordas")
    create_data_type_scalar   ("PortNumber", "Integer", ["LargerThan", "SmallerThan"], [1024, 65236], "OPTAES", "cciordas")
    create_data_type_composite("ServiceAddress", {"host" : "String", "port" : "PortNumber"}, "OPTAES", "cciordas")
    create_data_item          ("OOS4.PULSE_PORT", "ServiceAddress", "OPTAES", "cciordas")
    create_incarnation        ("OOS4.PULSE_ADDR", "oaes-nj2-12", {"hostname":"localhost"  ,"port":18004}, "OPTAES", "cciordas")
    update_incarnation        ("OOS4.PULSE_ADDR", "oaes-nj2-12", {"hostname":"oaes-arm-02","port":18004}, "OPTAES", "cciordas")
    update_incarnation        ("OOS4.PULSE_ADDR", "oaes-nj2-12", {"hostname":"oaes-pdc-11","port":18004}, "OPTAES", "cciordas")    
    delete_vault              ("OPTAES", "cciordas")
