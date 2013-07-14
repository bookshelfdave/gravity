import argparse
import threading
import os.path
import fnmatch
import sqlite3

#class LogFileParser(threading.Thread):
class LogFileParser():
  def __init__(self, logfile, args):
    #threading.Thread.__init__(self)
    self.logfile = logfile
    self.args = args

  def run(self):
    self.conn = sqlite3.connect(self.args.db)
    msg = "Processing file {}\n".format(self.logfile.filename)
    print msg,
    f = open(self.logfile.filename, 'r')
    lastlog = None
    for line in f:
      # this logic is brilliant and lazy!
      if len(line) > 10 and line[4] == '-' and line[7] == '-':
        if lastlog == None:
          lastlog = line
        else:
          self.logdata(lastlog)
          lastlog = line
      else:
        if lastlog == None:
          lastlog = line
        else:
          lastlog = lastlog + line
      self.logdata(lastlog)

  # this is not elegant at the moment, but I'm in a hurry
  def parsedata(self, ls):
     chunks = ls.split(" ", 4)
     if len(chunks) > 4:
       etime = chunks[0]
       edate = chunks[1]
       edatetime = "{} {}".format(etime, edate)
       elevel = chunks[2][1:-1]
       epidmfl   = chunks[3]
       chunks2 = epidmfl.split("@")
       emsg = chunks[4]

       epid = ""
       emod = ""
       efun = ""
       eline = ""
       if len(chunks2) == 1:
         # only pid is available
         epid = chunks2[0]
       else:
         epid = chunks2[0]
         chunks3 = chunks2[1].split(":")
         emod  = chunks3[0]
         efun  = chunks3[1]
         eline = chunks3[2]
       return dict( event_date = edatetime,
                    event_msg = emsg,
                    event_module = emod,
                    event_fun = efun,
                    event_node = self.logfile.nodename,
                    event_cluster = self.logfile.clustername,
                    event_filesource = self.logfile.filename)
     else:
       return {}

  def logdata(self, ls):
    d = self.parsedata(ls)

    if len(d) == 0:
      return

    sql = """insert into gravity (event_date,
                                  event_msg,
                                  event_module,
                                  event_fun,
                                  event_node,
                                  event_cluster,
                                  event_filesource) VALUES
                (?,?,?,?,?,?,?)"""
    params = (d["event_date"],
              d["event_msg"],
              d["event_module"],
              d["event_fun"],
              d["event_node"],
              d["event_cluster"],
              d["event_filesource"])
    self.conn.execute(sql,params)
    self.conn.commit()

class LogFile:
  def __init__(self, filename, nodename, clustername):
    self.filename = filename
    self.nodename = nodename
    self.clustername = clustername

class GravitySchema:
  def __init__(self, args):
    self.args = args

  def create(self):
    print "Initializing", self.args.db
    conn = sqlite3.connect(self.args.db)
    conn.execute('''create table gravity
                      (event_date text,
                       event_msg text,
                       event_level text,
                       event_module text,
                       event_fun text,
                       event_node text,
                       event_cluster text,
                       event_filesource text,
                       event_tag1 text,
                       event_tag2 text)''')
    conn.close()

  def exists(self):
    if os.path.exists(self.args.db):
      return True
    else:
      return False

class LogClassifier:
  def __init__(self, args):
    self.args = args
    self.conn = sqlite3.connect(self.args.db)
    self.classifiers = []

  def add_classifier(self, c):
    self.classifiers.append(c)

  def process(self):
    for row in self.conn.execute("select * from gravity order by date(event_date) asc"):
      for c in self.classifiers:
        c.process(row)


class SimpleClassifier:
  def process(self, row):
    print row[1],

print "Gravity v0.1"

parser = argparse.ArgumentParser(description='Gravity log analysis')
parser.add_argument("--serve", metavar='serve_port', help="serve stats on specified port")
parser.add_argument('--scan', nargs='?', metavar='scandir', default="", help='recursively scan a directory tree for log files')
parser.add_argument("--cluster", metavar='cluster', help="cluster", default="local")
parser.add_argument("--db", metavar='database_name', help="database name", default="gravity.db")
args = parser.parse_args()

s = GravitySchema(args)
if not s.exists():
  s.create()

print "Cluster", args.cluster


logfiles = []

valid_prefixes=['console','error']

if args.scan != "":
  print "Scanning", args.scan
  matches = []
  for root, dirnames, filenames in os.walk(args.scan):
    for filename in filenames:
      nodename = os.path.basename(root)
      for prefix in valid_prefixes:
        if fnmatch.fnmatch(filename, prefix + '.*'):
          f = os.path.join(root, filename)
          l = LogFile(f, nodename, args.cluster)
          logfiles.append(l)

#for f in args.logs:
#  if os.path.exists(f):
#    logfiles.append(LogFileParser(f, args))
#  else:
#    raise Exception("Invalid file " + f)
#

for lf in logfiles:
  p = LogFileParser(lf,args)
  p.run()

lc = LogClassifier(args)
lc.add_classifier(SimpleClassifier())
lc.process()

