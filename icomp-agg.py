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
    dbpath = None
    
    parse = argparse.ArgumentParser()
    parse.add_argument('-f','--file',help='file = IC Report file and path')
    parse.add_argument('-l','--list',help='list = List of IC Report files')
    parse.add_argument('-p','--print',help='print = Print database to STDOUT')
    parse.add_argument('-x','--excel',help='excel = Output DB to Excel spreadsheet')
    parse.add_argument('-d','--db',const='icompdb.sql', nargs='?', help='db = Open DB, default=icompdb.sql')
    parse.add_argument('-v','--verbose',help='verbose = Verbose',action='store_true')
    
    program_args = parse.parse_args()

    if program_args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    dbpath = program_args.db
    logging.info("Opening " + dbpath)
    dbic = DB(dbpath)
    
        
    if program_args.list is not None:
        xllist = program_args.list
    elif program_args.file is not None:
        xllist = []
        xllist.append(program_args.file)

    if xllist is not None:
        for xlpath in xllist:
            report = Report(xlpath)
            dbic.add_report(report.date,report.count,xlpath)
            for repitem in report.report_items:
                dbic.add_claim(report.date,report.report_items[repitem])            

class Report:
    def __init__(self,reportfile):
        self.filename = reportfile
        self.parse_report(reportfile)

    def parse_report(self,loadpath):
        logging.debug ("Loading Report from Excel file at " + loadpath)
        wbk = load_workbook(filename=loadpath)
        sheet0 = wbk._sheets[0]
        report_date_cell = sheet0[2][0].value
        rd_split = re.split(', | ',report_date_cell)
        report_date_string = rd_split[1] + " " + rd_split[2] + " " + rd_split[3]
        report_date_object = datetime.strptime(report_date_string,'%B %d %Y')
        report_rows = sheet0.rows
        lrows = sys.getsizeof(report_rows)
        self.date = report_date_object
        self.count = lrows - 2
        irow = 1
        report_items = {}
        for rr in report_rows:
            if irow > 3:
                proc_no = rr[1].value
                proc_no = re.sub('\n','',proc_no)
                intervenor = rr[2].value
                intervenor = re.sub('\n',' ',intervenor)
                claim_date = rr[3].value
                claim_amount = rr[4].value
                status = rr[5].value
                claim_key = (intervenor,claim_date) 
                report_items[(intervenor, claim_date)] =  {'intervenor' : intervenor, 'claim_date' : claim_date, 'proc_no' : proc_no, 'claim_amount' : claim_amount, 'status' : status}
                logging.debug("   "+ intervenor + "  " + str(claim_date) + "  " + proc_no + "  " +
                              str(claim_amount) + "  " + status)
            irow +=1

        self.report_items = report_items
        logging.info("Loaded Report from Excel file at " + loadpath + "  Dated " + report_date_string)

    def get_db_report(self,db):
        logging.info("Loading Report from SQL DB at " + dbpath)

    def put_db_report(self,db):
        logging.info("Adding Report to SQL DB at " + dbpath)


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
            self.create(dbpath)
        else:
            self.open(dbpath)
    
    def create(self,dbpath):
        if os.path.isfile(dbpath):
            raise FileExistsError(dbpath + " already exists")
        logging.info("Creating " + dbpath)
        dbfile = open(dbpath,'w')
        dbfile.close()
        self.open(dbpath)
        # Create report table
        sql = "CREATE TABLE report ( rdate DATE, count INT, filename STRING, PRIMARY KEY (rdate));"
        logging.debug(sql)
        self.cursor.execute(sql)
        # Create claim table
        sql = "CREATE TABLE claim ( cmdate DATE, frdate DATE, lrdate DATE, intervenor VARCHAR(30), proceeding VARCHAR(30), " \
            "amount INT, status VARCHAR(10), cldate DATE, duration INT, PRIMARY KEY (cmdate, intervenor), " \
            "FOREIGN KEY (frdate) REFERENCES rdate (lrdate), FOREIGN KEY (lrdate) REFERENCES rdate (lrdate));"
        logging.debug(sql)
        self.cursor.execute(sql)
        self.connection.commit()
        
    def open(self,dbpath):
        logging.info("Opening DB " + dbpath)
        try:
            connection = sqlite3.connect(dbpath)
        except Error as e:
            logging.error(e)
        self.connection = connection
        self.cursor = connection.cursor()
        self.db_name = dbpath

    def get_report(self,rptdate):
        sql = '''SELECT * FROM report WHERE rdate = ?'''
        logging.debug(sql)
        sqldate = rptdate.date().isoformat()
        self.cursor.execute(sql,(sqldate,))
        report = self.cursor.fetchone()
        return report
        
    def add_report(self,rdate, count, filename):
        sql = '''INSERT INTO report (rdate, count, filename) VALUES (?,?,?)'''
        logging.debug(sql)
        report_check = self.get_report(rdate)
        if report_check == None:
            sqldate = rdate.date().isoformat()
            self.cursor.execute(sql,(sqldate,count,filename))
            self.connection.commit()

    def get_claim(self,cdt,ivn):
        sql = '''SELECT * FROM claim WHERE cmdate = ? AND intervenor = ?'''
        logging.debug(sql)
        sqldate = cdt.date().isoformat()
        self.cursor.execute(sql,(sqldate,ivn))
        claim = self.cursor.fetchone()
        return claim

    def add_claim(self,rdate,ritem):
        sql = '''INSERT INTO claim (cmdate, frdate, lrdate, intervenor, amount, proceeding, status, cldate, duration) VALUES (?,?,?,?,?,?,?,?,?)'''
        logging.debug(sql)
        claim_check = self.get_claim(ritem['claim_date'],ritem['intervenor'])
        if claim_check == None:
            sqlrdt = rdate.date().isoformat()
            sqlcdt = ritem['claim_date'].date().isoformat()
            self.cursor.execute(sql,(sqlcdt,sqlrdt,sqlrdt,ritem['intervenor'],ritem['claim_amount'],ritem['proc_no'],ritem['status'],None,None))
            self.connection.commit()
    
    def export(xlpath):
        logging.info("Writing DB to " + xlpath)


if __name__ == "__main__":
    main()
