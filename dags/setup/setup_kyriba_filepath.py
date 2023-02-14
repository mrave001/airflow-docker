import glob
import os

    
def wipPath():
    csv_dir = os.getcwd() + "/WIP"
    os.chdir(csv_dir)
    path = []
    for file in glob.glob("*NS*.csv"):
        path.append(os.getcwd() + "/" + file)
    return path

def wipPath_dag(wipPath):
    path = []
    for file in glob.glob("*NS*.csv"):
        path.append(os.getcwd() + "/" + file)
    return path

def ns2_csvPath():
    for file in glob.glob("*NS2*.csv"):
        ns2_csvpath = os.getcwd() + "/" + file
    return ns2_csvpath

def ns3_csvPath():
    for file in glob.glob("*NS3*.csv"):
        ns3_csvpath = os.getcwd() + "/" + file
    return ns3_csvpath
