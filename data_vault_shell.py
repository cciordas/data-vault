#! /usr/bin/env python

from pymongo        import Connection
from pymongo.errors import ConnectionFailure
from cmd2           import Cmd
from cmd_create_datatype_simple import cmd_create_datatype_simple
import sys

class CLI(Cmd):
        
    def do_connect(self, line):
        """ 
        Connect to the MongoDB backend
        @param host hostname
        @param port port 
        """
        print "\n connecting to MongoDB backend ...\n"

    def do_create_datatype_simple(self, line):
        cmd_create_datatype_simple(self)
        
    def emptyline(self):
        pass
        
    def do_EOF(self, line):
        print Cmd.colorize("\nExiting...\n", red)
        return True
    


cxn = None
try:
    cxn = Connection(host="localhost", port=27017)
except ConnectionFailure, e:
    err = "Could not connect to MongoDB: %s\n" % e


intro  = "\nDataVault version 0.0.1 (2015-07-03) -- \"Secret Keeper\"\n"
intro += "Copyright (C) 2015 Calin Ciordas\n"
intro += "Type \"help\" for more information\n\n"

if cxn: intro += "Connected to MongoDB backend on locahost:27017\n"
else:   intro += err
    
cli = CLI()
cli.multilineCommands = ['create_data_type']
cli.intro  = intro
cli.prompt = "vault> "
cli.ruler  = "-"
if cxn:
    cli.db = cxn["data_vault"]
    cli.cmdloop()
else:
    sys.stdout.write(intro + "Exiting...\n\n")
    sys.exit(1)
