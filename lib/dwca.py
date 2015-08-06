import zipfile
from lxml import etree
import sys
from collections import deque
import xmlDictTools
import os
import chardet

FNF_ERROR = "File {0} not present in archive."

NAMESPACES = { "http://rs.tdwg.org/dwc/terms/": "dwc:",
               "http://purl.org/dc/terms/": "dcterms:",
               "http://rs.tdwg.org/ac/terms/": "ac:",
               "http://ns.adobe.com/xap/1.0/rights/": "xmpRights:",
               "http://ns.adobe.com/xap/1.0/": "xmp:",
               "http://iptc.org/std/Iptc4xmpExt/1.0/xmlns/": "Iptc4xmpExt:",
               "http://portal.idigbio.org/terms/": "idigbio:",
               "http://symbiota.org/terms/": "symbiota"
             }

def archiveFile(archive,name):
    metaname = name
    for f in archive.namelist():
        if f.endswith(name):
            metaname = f
    return metaname

class Dwca:
    """
        Internal representation of a Darwin Core Archive file.
    """
    
    archdict = None
    archive = None   
    metadata = None
    core = None
    extensions = None

    def __init__(self,name="dwca.zip",skipeml=False):
        self.archive = zipfile.ZipFile(name, 'r')

        self.archive.extractall(name.split(".")[0])
            
        rdict = {}    
        with open(name.split(".")[0] + "/" + archiveFile(self.archive,"meta.xml"),'r') as meta:
            root = etree.parse(meta).getroot()
            rdict = xmlDictTools.xml2d(root)
                
        self.archdict = rdict["archive"]

        if not skipeml and "#metadata" in self.archdict:
            metadata = archiveFile(self.archive,self.archdict["#metadata"])
            with open(name.split(".")[0] + "/" + metadata,'r') as mf:
                mdtree = etree.parse(mf).getroot()           
                self.metadata = xmlDictTools.xml2d(mdtree)
        else:
            self.metadata = None

        corefile = archiveFile(self.archive,self.archdict["core"]["files"]["location"])
        self.core = DwcaRecordFile(self.archdict["core"], name.split(".")[0] + "/" + corefile)
        
        self.extensions = []
        if "extension" in self.archdict:
            if isinstance(self.archdict["extension"],list):
                for x in self.archdict["extension"]:
                    extfile = archiveFile(self.archive,x["files"]["location"])
                    try:
                        self.extensions.append(DwcaRecordFile(x, name.split(".")[0] + "/" + extfile))
                    except:
                        pass
            else:            
                extfile = archiveFile(self.archive,self.archdict["extension"]["files"]["location"])
                self.extensions.append(DwcaRecordFile(self.archdict["extension"], name.split(".")[0] + "/" + extfile))

class DwcaRecordFile:
    """
        Internal representation of a darwin core archive record data file.
    """

    name = ""
    filehandle = None
    closed = True
    namespace = ""
    filetype = ""
    rowtype = ""
    encoding = "utf-8"
    linesplit = "\n"
    fieldsplit = "\t"
    fieldenc = ""
    ignoreheader = 0
    defaults = None
    # Don't instantiate objects here
    fields = None 
    linebuf = None
    def __init__(self,filedict,fh):
        """
            Construct a DwcaRecordFile from a xml tree pointer to the <location> tag containing the data file name
            and a file handle or string pointing to the data file.
        """
              
        self.name = filedict['files']['location']

        # Instantiate objects here or we get cross talk between classes.
        self.fields = {}
        self.linebuf = deque()
        closed = False

        idtag = "id"
        if 'id' in filedict:
            self.filetype = "core"
        else:
            idtag = "coreid"
            self.filetype = "extension"
        self.rowtype = filedict["#rowType"]
        self.encoding = filedict["#encoding"]
        self.linesplit = filedict["#linesTerminatedBy"].decode('string_escape') 
        self.fieldsplit = filedict["#fieldsTerminatedBy"].decode('string_escape') 
        self.fieldenc = filedict["#fieldsEnclosedBy"].decode('string_escape') 
        self.ignoreheader = int(filedict["#ignoreHeaderLines"])


        if isinstance(fh,str) or isinstance(fh,unicode):
            # with open(fh,'rb') as df:
            #     cdresult = chardet.detect(df.read())                
            #     if self.encoding != cdresult['encoding']:
            #         print "encoding mismatch", cdresult['encoding'], self.encoding
            #         self.encoding = cdresult['encoding']
            self.filehandle = open(fh,'rb')
        else:
            self.filehandle = fh

        idfld = filedict[idtag]
        self.fields[int(idfld['#index'])] = idtag
        self.defaults = {}
        if not isinstance(filedict['field'],list):
            filedict['field'] = [filedict['field']]
        for fld in filedict['field']:
            term = fld['#term']
            for ns in NAMESPACES:
                if term.startswith(ns):
                    term = term.replace(ns,NAMESPACES[ns])
                    break
            if '#index' in fld:
                self.fields[int(fld['#index'])] = term
            elif '#default' in fld:
                self.defaults[term] = fld['#default']
            else:
                raise Exception("Field {0} has neither index nor default in {1}".format(term,self.name))

    def __iter__(self):
        """
            Returns the object itself, as per spec.
        """
        return self

    def close(self):
        """
            Closes the internally maintained filehandle
        """
        self.filehandle.close()
        closed = self.filehandle.closed

    def next(self):
        """
            Returns the next line in the record file, used for iteration
        """
        r = self.readline()
        if r != None:
            if len(r) > 0:       
                return r
            else:
                raise StopIteration
        else:
            return self.next()

    def readline(self,size=None):
        """
            Returns a parsed record line from a DWCA file as an dictionary.
        """
        while len(self.linebuf) == 0:  
            try:          
                fileLine = self.filehandle.readline().decode(self.encoding)
            except:
                continue
            if len(fileLine) == 0:
                return {}
            else:
                fileLineArr = fileLine.split(self.linesplit)
                for potLine in fileLineArr:
                    if len(potLine) > 0:
                        if self.ignoreheader == 0:
                            self.linebuf.append(potLine)
                        else:
                            self.ignoreheader -= 1
                        
        
        line = self.linebuf.popleft()
        # DRAGON WARNING temporary hack to fix broken quotes in ed's CSVs
        line = line.replace("\"\"","\\\"")
        lineArr = []
        infield = False
        i = 0
        buff = ""
        while i < len(line):
            c = line[i]
            # special handling for escaped quotes
            if c == "\\":
                if line[i+1] in [self.fieldenc, "\\"]:
                    c += line[i+1]
                    i += 1                

            if c == self.fieldenc and not infield:
                infield = True
            elif c == self.fieldenc and infield:
                infield = False
                lineArr.append(buff)
                buff = ""
                i += 1
            elif c == self.fieldsplit and not infield:
                lineArr.append(buff)
                buff = ""
            else:
                buff += c
            i += 1
        lineArr.append(buff)

        lineDict = {}        
        for k in self.fields:
            try:
                if lineArr[k] != "":
                    lineDict[self.fields[k]] = lineArr[k]
            except IndexError, e:
                print "Line missing fields: ", line
                print k, self.fields[k]
                print lineArr, len(lineArr)
                return None
        lineDict.update(self.defaults)
        return lineDict

    def readlines(self,sizehint=None):
        """
            Returns all lines in the file. Cheats off readline.
        """
        lines = []
        for line in self:
            lines.append(self.readline())
        return lines