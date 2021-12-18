###
# This script aggregates quarterly intervenor compensation reports into a
# database containing all intervenor compensation claims, including history and
# and current status.

import os
import os.path
import sys
import re
import sqlite3
from datetime import datetime
import logging
from openpyxl import load_workbook
import xlrd

def main():
    import argparse

    report = None
    dbic = None
    
    parse = argparse.ArgumentParser()
    parse.add_argument('-f','--file',help='file = IC Report file and path')
    parse.add_argument('-l','--list',help='list = List of IC Report files')
    parse.add_argument('-p','--print',help='print = Print database to STDOUT')
    parse.add_argument('-x','--excel',help='excel = Output DB to Excel spreadsheet')
    parse.add_argument('-d','--db',default='icompdb.sql',help='db = Open DB, default=icompdb.sql')
    parse.add_argument('-c','--create',default='icompdb.sql',help='create = Create DB, default=icompdb.sql')
    parse.add_argument('-v','--verbose',help='verbose = Verbose',action='store_true')
    
    program_args = parse.parse_args()

    if program_args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if program_args.create is not None:
        dbpath = program_args.create
        logging.info("Opening " + dbpath)
        dbic = DB.create(dbpath)
        
    if program_args.list is not None:
        xllist = program_args.list
    elif program_args.file is not None:
        xllist = []
        xllist.append(program_args.file)

    if xllist is not None:
        for xlpath in xllist:
            report = Report.parse_report(xlpath)
            
    

class Report:
    def __init__(self,reportfile,rdt,rilist):
        self.filename = reportfile

    def parse_report(loadpath):
        logging.debug ("Loading Report from Excel file at " + loadpath)
        wbk = load_workbook(filename=loadpath)
        sheet0 = wbk._sheets[0]
        report_date_cell = sheet0[2][0].value
        rd_split = re.split(', | ',report_date_cell)
        report_date_string = rd_split[1] + " " + rd_split[2] + " " + rd_split[3]
        report_date_object = datetime.strptime(report_date_string,'%B %d %Y')
        report_rows = sheet0.rows
        lrows = sys.getsizeof(report_rows)
        irow = 1
        report_list = []
        for rr in report_rows:
            if irow > 3:
                proc_no = rr[1].value
                intervenor = rr[2].value
                intervenor = re.sub('\n',' ',intervenor)
                claim_date = rr[3].value
                claim_amount = rr[4].value
                status = rr[5].value
                report_row = [proc_no, intervenor, claim_date, claim_amount, status]
                logging.debug("   "+ report_row[0] + "  " + report_row[1] + "  " +
                              str(report_row[2]) + "  " + str(report_row[3]) + "  " + report_row[4])
                report_list.append(report_row)
            irow +=1

        report_object = Report(loadpath,report_date_object,report_list)            
        logging.info("Loaded Report from Excel file at " + loadpath + "  Dated " + report_date_string)
        return report_object

    def get_db_report(self,db):
        logging.info("Loading Report from SQL DB at " + dbpath)

    def put_db_report(self,db):
        logging.info("Adding Report to SQL DB at " + dbpath)


class ReportItem:
    def __init__(self,intv,dt,amt,sts):
        self.intervenor = intv
        self.date = dt
        self.amount = amt
        self.status = sts

class Claim:
    def __init__(self,frdt,lrdt,ri):  #First report date, last report date, ReportItem
        self.first_report = frdt
        self.last_report = lrdt
        self.intervenor = ri.intervenor
        self.date = ri.date
        self.amount = ri.amount
        self.status = ri.status        
    
class DB:
    def __init__(self,dbpath):
        if not os.path.isfile(dbpath):
            self.create(self,dbpath)
        else:
            self.open(dbpath)
    
    def create(dbpath):
        if os.path.isfile(dbpath):
            raise FileExistsError(dbpath + " already exists")
        logging.info("Creating " + dbpath)
        dbfile = open(dbpath,'w')
        dbfile.close()
        icdb = DB(dbpath)
        # Initialization of tables goes here
        return icdb
        
    def open(self,dbpath):
        logging.info("Opening DB " + dbpath)
        try:
            connection = sqlite3.connect(dbpath)
        except Error as e:
            logging.error(e)
        self.connection = connection
        self.cursor = connection.cursor()
        self.db_name = dbpath
        return self
        
    def export(xlpath):
        logging.info("Writing DB to " + xlpath)


if __name__ == "__main__":
    main()
