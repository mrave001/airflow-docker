
import re
from setup.setup_kyriba_filepath import *

# This function finds the start and end positions of all occurrences of "NS2" envmnt value  
def NS2_envm_value():
    path = ns2_csvPath()
    for match in re.finditer("NS2", path):
        envmnt_value = path[match.start():match.end()]
    #print(str(envmnt_value))    
    
    return str(envmnt_value)

# This function finds the start and end positions of all occurrences of "NS2" envmnt value 
def NS3_envm_value():
    path = ns3_csvPath()
    for match in re.finditer("NS3", path):
        envmnt_value = path[match.start():match.end()]
    #print(str(envmnt_value))    
    
    return str(envmnt_value)