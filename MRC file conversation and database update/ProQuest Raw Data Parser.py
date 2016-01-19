import re,os,sys,csv
from pymarc import MARCReader





fd = '' #please specify folder with raw ProQuest files
fd2 = '' #please specify output folder



def read(ff):
    reader = MARCReader(ff)
    num = 0
    for r in reader:
        try:
            num+=1
            pubnum = str(r['001']).replace("=001  ",'')
            latesttransaction = str(r['005']).replace("=005  ",'')
            isbn = r.isbn()
            author = r.author()
            try: 
                fortrans = r['242']['a']
            except:
                fortrans = 'NULL'
            tit = r.title()
            try:
                advisor = re.sub('^\W+','',re.search('Adviser\:(.*?)\.\n',str(r)).group(1))
            except:
                try:
                    advisor = re.sub('^\W+','',re.search('Advisers\:(.*?)\.\n',str(r)).group(1))
                except:
                    advisor = 'NULL'
            dissnote = r['502']['a']
            abstr = ''
            for rr in str(r).decode('utf-8','ignore').split("\n"):
                if rr.startswith('=520'):
                    if abstr == '':
                        abstr+=re.sub('\=520.*?(?=[A-Z])','',rr)
                    else:
                        abstr+=' '+re.sub('\=520.*?(?=[A-Z])','',rr)
            schoolcode = re.search('\d+',r['590']['a']).group()
            subjects = ''
            for rr in str(r).decode('utf-8','ignore').split("\n"):
                if rr.startswith('=650'):
                    if subjects == '':
                        subjects+=re.sub('\=650.*?(?=[A-Z])','',rr)
                    else:
                        subjects+=' '+re.sub('\=650.*?(?=[A-Z])','',rr)
            addedname = ''
            for rr in str(r).decode('utf-8','ignore').split("\n"):
                if rr.startswith('=700'):
                    if addedname == '':
                        addedname+=re.sub('\=700.*?(?=[A-Z])','',rr)
                    else:
                        addedname+=' '+re.sub('\=700.*?(?=[A-Z])','',rr)
            addedname = addedname.replace('$e',' ')
            corpname = ''
            for rr in str(r).decode('utf-8','ignore').split("\n"):
                if rr.startswith('=710'):
                    if corpname == '':
                        corpname+=re.sub('\=710.*?(?=[A-Z])','',rr)
                    else:
                        corpname+=' '+re.sub('\=710.*?(?=[A-Z])','',rr)
            corpname = corpname.replace('$e',' ')
            try:
                varianttit = r['740']['a']
            except:
                varianttit = 'NULL'
            
            degree = r['791']['a']
            degreedate = r['792']['a']
            try:
                lang = r['793']['a']
            except:
                lang = 'English'
            
            outp.writerow([pubnum,latesttransaction,isbn,author,tit,fortrans,advisor,dissnote,abstr,str(schoolcode),subjects,addedname,corpname,varianttit,degree,degreedate,lang])
        except:
            print num
    print num


# The following function is only for subject code extraction if needed and retrieves a bit more than the basic read() function above
def read_code(ff):
    reader = MARCReader(ff)
    num = 0
    for r in reader:
        try:
            num+=1
            pubnum = str(r['001']).replace("=001  ",'')
            subjects = ''
            subjectcodes = ''
            for rr in str(r).decode('utf-8','ignore').split("\n"):
                if rr.startswith('=650'):
                    if subjects == '':
                        subjects+=re.sub('\=650.*?(?=[A-Z])','',rr)
                    else:
                        subjects+=' '+re.sub('\=650.*?(?=[A-Z])','',rr)
                if rr.startswith('=690'):
                    if subjectcodes == '':
                        subjectcodes+=re.sub('\=690.*?(?=[0-9])','',rr)
                    else:
                        subjectcodes+=' '+re.sub('\=690.*?(?=[0-9])','',rr)
            
            outp.writerow([pubnum,subjects,subjectcodes])
        except:
            print num
    print num
    
diri = os.listdir(fd)
for d in diri:
    if d.startswith('1999') or d.startswith('200'):
        print d
        ff = open(fd+d)
        outp = csv.writer(open(fd2+d.replace('.MRC','')+'.csv','wb'))
        outp.writerow(['Publication Number','Latest Transaction Date','ISBN','Author','Title','Translated Title','Advisors','Dissertation Note','Abstract','School Code','Subjects','Added Author Names','Corporate Name','Variant Title','Degree','Degree Date','Language'])
        read(ff)