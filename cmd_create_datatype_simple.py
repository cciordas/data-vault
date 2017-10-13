from cmd2 import Cmd
import json

def cmd_create_datatype_simple(cmd):

    name  = raw_input("\n data type name: ")
    if cmd.db.datatypes_simple.find({"name" : name}).count() > 0:
        print cmd.colorize(" ERROR: datatype '%s' already exists\n" % name, "red")
        return
    
    base  = raw_input(" base type     : ")
    if cmd.db.datatypes_simple.find({"name" : base}).count() == 0:
        print cmd.colorize(" ERROR: datatype '%s' does not exist\n" % base, "red")
        return

    
    descr = raw_input(" description   : ")

    
    print "\n storage type: "
    dtype = cmd.select(["integer", "double", "string"], "   ")

    constraints = []
    while True:
        constraint = _get_constraint(cmd)
        if constraint == ():
            break
        constraints.append(constraint)
        
    d = {"name" : name, "type" : dtype, "description" : descr, "constraints" : constraints}
    print "\n creating simple data type:"
    print "\n" + json.dumps(d, indent=2, separators=(',', ' : '))
    
    cmd.db.datatypes_simple.insert(d)
    #print cmd.colorize("creating type '%s'" % type, "blue")


def _get_constraint(cmd):

    print "\n constraint: "
    constr = cmd.select(["gt", "gte", "lt", "lte", "none"], "   ")
    if constr == "none":
        return ()
    else:
        value = raw_input("\n value: ")
    return (constr, value)


    
