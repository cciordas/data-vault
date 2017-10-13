#! /usr/bin/env python

from return_value import ReturnValue
import logging

"""
Implements the Data Vault.
"""

class DataVault:
    """
    A managed data store.
    """

    def __init__(self, vaultID):
        """
        @param vaultID -- name of the Data Vault
        """

        # stores all Data Types available in this vault (class name as key, class as value)
        self.DTs = {}

        # stores all Data Items stored in this vault (class name as key, class as value)
        DIs = {}

        # stores Data Item incarnations in a two-leveled dictionary:
        # the first key is the Data Item name, the second is the incarnation key
        DIincarnations = {}

        # initialize logging
        self.logger = logging.getLogger("DataVault-%s" % vaultID)
        self.logger.setLevel(logging.DEBUG)
        ch = logging.FileHandler("/Users/cciordas/tmp/datavault.log")
        ch.setLevel(logging.DEBUG)
        fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s: %(message)s", datefmt="%Y-%m-%d @ %H:%M:%S")
        ch.setFormatter(fmt)
        self.logger.addHandler(ch)


    def create_data_type_scalar(self, DTname, DTbase_name, DTCname, DTCvalue):
        """
        Create a new scalar Data Type.
        
        @param DTname      -- name of the new scalar Data Type
        @param DTbase_name -- name of the built-in scalar Data Type on which the new Data Type is based
        @param DTCname     -- name of the Data Type Constraint attached to the new Data Type
        @return success status via a ReturnValue
        """
        s = ""
        for i in range(len(DTCname)):
            s += (" and %s(%s)" if i > 0 else "%s(%s)") % (DTCname[i], DTCvalue[i])    
        logmsg = "creating scalar data type '%s': %s with constraints %s" % (DTname, DTbase_name, s)
        try:
            self.logger.debug(logmsg)
            return ReturnValue(None)
        except Exception as err:
            self.logger.error(logmsg + " %s" % err)
            return ReturnValue(None, False, str(err))


    def create_data_type_composite(self, DTname, datafields):
        """
        Create a new composite Data Type.
        
        @param DTname     -- name of the new composite Data Type
        @param datafields -- a (nested) dictionary storing the names of the data fields (as key) and their Data Type (as value)
        @return success status via a ReturnValue
        """
        logmsg = "creating composite data type '%s': %s" % (DTname, datafields)
        try:
            self.logger.debug(logmsg)
            return ReturnValue(None)
        except Exception as err:
            self.logger.error(logmsg + " %s" % err)
            return ReturnValue(None, False, str(err))            


    def create_data_item(self, DIname, DTname):
        """
        Create a new Data Item.
        
        @param DIname -- name of the new Data Item being created
        @param DTname -- name of the Data Type associated with the new Data Item.
        @return success status via a ReturnValue
        """
        logmsg = "creating data item '%s' of type '%s'" % (DIname, DTname)
        try:
            1/0
            self.logger.debug(logmsg)
            return ReturnValue(None)
        except Exception as err:
            self.logger.error(logmsg + " %s" % err)
            return ReturnValue(None, False, str(err))
        

    def create_data_item_incarnation(self, DIname, key, value):
        """
        Create a new incarnation (managed instance) of a Data Item.
        
        @param DIname -- the Data Item name
        @param key    -- unique (among incarnations of the same Data Item) incarnation ID 
        @param value  -- the initial value of the incarnation
        @return success status via a ReturnValue
        """
        logmsg = "creating incarnation of '%s' with key='%s'  & value='%s'" % (DIname, key, value)
        try:
            self.logger.debug(logmsg)
            return ReturnValue(None)
        except Exception as err:
            self.logger.error(logmsg + " %s" % err)
            return ReturnValue(None, False, str(err))
        

    def update_incarnation(self, DIname, key, value):
        """
        Updates the value of an existing Data Item incarnation.
        
        @param DIname -- the Data Item name
        @param key    -- unique (among incarnations of the same Data Item) incarnation ID
        @param value  -- the updated value of the incarnation
        @return success status via a ReturnValue
        """
        logmsg = "updating incarnation of '%s' with key='%s' to value='%s'" % (DIname, key, value)
        try:
            self.logger.debug(logmsg)
            return ReturnValue(None)
        except Exception as err:
            self.logger.error(logmsg + " %s" % err)
            return ReturnValue(None, False, str(err))

if __name__ == "__main__":
    data_vault = DataVault("OPTAES")
