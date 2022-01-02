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
    parse.add_argument('-d','--db',const='data/icomp.db', nargs='?', help='db = Open DB, default=data/icomp.db')
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
        xllist = []
        xlfile = open(program_args.list,"r")
        xl0 = xlfile.readlines()
        for xf in xl0:
            xllist.append(xf.strip('\n'))
    elif program_args.file is not None:
        xllist = []
        xllist.append(program_args.file)

    if xllist is not None:
        for xlpath in xllist:
            report = Report(xlpath)
            dbic.add_report(report.date,report.count,xlpath)
            for repitem in report.report_items:
                dbic.update_claim(report.date,report.report_items[repitem])
                dbic.add_claim(report.date,report.report_items[repitem])
            dbic.close_missing_claims(report.date)

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
        if claim != None:
            claiminit = {'cmdate' : '','frdate' : '','lrdate' : '','intervenor' : '','amount' : '','proceeding' : '','status' : '','cldate' : '','duration' : ''}
            claim = dict(zip(claiminit,claim))
        return claim

    def add_claim(self,rdate,ritem):
        sql = '''INSERT INTO claim (cmdate, frdate, lrdate, intervenor, amount, proceeding, status, cldate, duration) VALUES (?,?,?,?,?,?,?,?,?)'''
        logging.debug(sql)
        claim_check = self.get_claim(ritem['claim_date'],ritem['intervenor'])
        (dbstat,cldate) = self.check_status(rdate,ritem['status'])
        duration = None
        if cldate != None:
            duration = (cldate - ritem['claim_date']).days
            cldate = cldate.date().isoformat()
        if claim_check == None:
            sqlrdt = rdate.date().isoformat()
            sqlcdt = ritem['claim_date'].date().isoformat()
            self.cursor.execute(sql,(sqlcdt,sqlrdt,sqlrdt,ritem['intervenor'],ritem['claim_amount'],ritem['proc_no'],dbstat,cldate,duration))
            self.connection.commit()

    def update_claim(self,rdate,ritem):
        rupdate = {}
        lupdate = []
        (dbstat,cldate) = self.check_status(rdate,ritem['status'])
        logging.debug("Status = " + dbstat)
        sqlrdt = rdate.date().isoformat()
        claim = self.get_claim(ritem['claim_date'],ritem['intervenor'])
        if claim != None:
            if claim['frdate'] > sqlrdt:
                rupdate['frdate'] = sqlrdt
            elif claim['lrdate'] < sqlrdt:
                rupdate['lrdate'] = sqlrdt
            if cldate != None:
                logging.debug(" Decision date = " + str(cldate))
                rupdate['cldate'] = cldate.date().isoformat()
                rupdate['duration'] = (cldate - ritem['claim_date']).days
                rupdate['status'] = dbstat
            elif dbstat != claim['status']:
                logging.debug(" Updating status to " + dbstat)
                rupdate['status'] = dbstat
        if len(rupdate) > 0:
            sql = "UPDATE claim SET "
            for upd in rupdate:
                sql = sql + upd + " = ?,"
                lupdate.append(rupdate[upd])
            sql = sql.strip(',')
            sql = sql + " WHERE intervenor = ? AND cmdate = ? AND status != 'Closed'"
            lupdate.extend([ritem['intervenor'],ritem['claim_date'].date().isoformat()])
            tpupdate = tuple(lupdate)
            logging.debug(sql + str(tpupdate))
            self.cursor.execute(sql,tpupdate)
            self.connection.commit()
        return

    def close_missing_claims(self,rdate):
        sqlrdt = rdate.date().isoformat()
        sql = "SELECT * FROM claim WHERE status != 'Closed'"
        logging.debug(sql)
        self.cursor.execute(sql)
        claimlist = self.cursor.fetchall()
        for claim in claimlist:
            logging.debug(str(claim))
            cmdate = datetime.strptime(claim[0],"%Y-%m-%d")
            lrdate = datetime.strptime(claim[2],"%Y-%m-%d")
            intervenor = claim[3]
            status = claim[6]
            if rdate > lrdate:
                duration = (rdate - cmdate).days
                sql = "UPDATE claim SET status = 'Closed', cldate = ?, lrdate = ?, duration = ? WHERE intervenor = ? AND cmdate = ?"
                logging.debug(sql + " :" + sqlrdt+"," + sqlrdt + "," + str(duration) + "," + intervenor + "," + str(cmdate))
                self.cursor.execute(sql,(sqlrdt,sqlrdt,duration,intervenor,cmdate.date().isoformat()))
            self.connection.commit()
    
    def check_status(self,rdate,clstat):
        ryear = rdate.year
        cldate = None
        dbstat = None
        agdt = re.search(r"On (\w+) (\d+)\w* Agenda",clstat)
        agdt2 = re.search(r"On (\d+)/(\d+)\w* Agenda",clstat)
        if re.search('Assigned',clstat):
            dbstat = 'Assigned'
        elif re.search('Pending',clstat):
            dbstat = 'Pending'
        elif re.search('Unassigned',clstat):
            dbstat = 'Pending'
        elif re.search('Not',clstat):
            dbstat = 'Pending'
        elif agdt != None:
            clmonth = agdt.group(1)
            clday = int(agdt.group(2))
            cldate = datetime.strptime(clmonth,"%B")
            cldate = cldate.replace(year=ryear).replace(day=clday)
            dbstat = 'Closed'
        elif agdt2 != None:
            clmonth = int(agdt2.group(1))
            clday = int(agdt2.group(2))
            cldate = datetime.now()
            cldate = cldate.replace(year=ryear).replace(month=clmonth).replace(day=clday)
            dbstat = 'Closed'
        else:
            raise ValueError(clstat + " - is not an expected value")
        return (dbstat,cldate)
            
    def export(xlpath):
        logging.info("Writing DB to " + xlpath)


if __name__ == "__main__":
    main()
