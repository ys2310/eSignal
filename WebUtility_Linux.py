#pip install pandas
#pip install lxml
#pip install html5lib
#pip install BeautifulSoup4
# http://stackoverflow.com/questions/38447738/beautifulsoup-html5lib-module-object-has-no-attribute-base
#pip install html5lib --upgrade
#pip install BeautifulSoup4 --upgrade

import io
import sys
sys.path.append('../PyElasticsearch/')

#import PyBulkApi
import IoUtility as iou
import OsUtility as osu
import DataTypeUtility as dpu

import time
import pandas as pd

#import html5lib
from bs4 import  BeautifulSoup 
from urllib.request import Request, urlopen
import urllib.request as ur

#from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
#from selenium.webdriver.support.ui import Select
#from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC

# ref : http://qiita.com/checkpoint/items/d9bcc63292d7f01c62d3
def download_file(download_url, _file_save_path):
    import requests
    r = requests.get(download_url)
    file = open(_file_save_path, 'wb')
    file.write(r.content)
    file.close()
    #print("Completed")

def get_html(_url):
    # http://stackoverflow.com/questions/16627227/http-error-403-in-python-3-web-scraping
    req = Request(_url, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req).read()
    #print(html)
    return html

def get_html_with_proxy(_url):
    proxy = ur.ProxyHandler({'http': '192.168.8.16:8080'})
    # construct a new opener using your proxy settings
    opener = ur.build_opener(proxy)
    # install the openen on the module-level
    ur.install_opener(opener)
    html = ur.urlopen(_url).read()
    return html

def send_gmail(_header_text, _body_text):
	# http://stackoverflow.com/questions/10147455/how-to-send-an-email-with-gmail-as-provider-using-python
    # http://kazuki-nagasawa.hatenablog.com/entry/python_email

    # Turn-on less-secured login if needed. (not-recommended)
    # https://www.google.com/settings/security/lesssecureapps
    #
    # More secure way by AOuth2. (recommended)
    # https://github.com/google/gmail-oauth2-tools/blob/master/python/oauth2.py
    # https://unoh.github.io/2007/06/13/python_2.html
    # 

	import smtplib
	from email.mime.text import MIMEText
	from email.header import Header
	from email import charset

	from_addr = 'luo2311@gmail.com'
	to_addrs  = 'ys2310@gmail.com'

	cset = 'utf-8'  # <---------------(文字セットの設定だよ)
	message = MIMEText(u'{0}'.format(_body_text), 'plain', cset)
	message['Subject'] = Header(u'{0}'.format(_header_text), cset)
	message['From'] = from_addr
	message['To'] = to_addrs

	con = smtplib.SMTP('smtp.gmail.com:587')
	con.set_debuglevel(True)
	con.starttls()
	username = 'luo2311'
	password = 'song1983s0me!nfo'
	con.login(username,password)
	con.sendmail(from_addr, [to_addrs], message.as_string()) 
	con.close()

def scrap_html_to_json(_driver, _folder):

    from elasticsearch import Elasticsearch
    es = Elasticsearch(host='localhost', port=9200)

    files = iou.getfiles(_folder, '.html')       

    for i, f in enumerate(files):

        # 進捗コンソール表示
        iou.console_title('scraping html into json {0}/{1}'.format(i, len(files)))

        #for url in urls:
        #try:
        # some characters might not be read even with utf-8 encoding
        try:
            print(_folder + f)
        except: # unrecognizable character in file path
            continue
        try:
            soup = BeautifulSoup(open(_folder + f, encoding='utf-8'), 'lxml') # 'lxml' parser often has problems with broken html in such case we use the 'html.parse' (default parser)
        except: # bad html files
            continue
        
        if soup.find('header', attrs={'class' : 'canvas-header'}) == None:  # html tags don't exist in saved source (JavaScropt generated? we get it via browser)                

            #print(soup.text.encode('utf-8'))
            try:
                url = 'http://finance.yahoo.com/news/' + soup.text.split('http://finance.yahoo.com/news/')[1].split('.html')[0] + '.html'
            except:
                print('List index out of range at {0}'.format(f))
                continue
            print(url)
            # use SELENIUM to select next menu (iBoxx index)
            # hidden browser : http://stackoverflow.com/questions/16180428/can-selenium-webdriver-open-browser-windows-silently-in-background
            # ChromwDriver options : http://stackoverflow.com/questions/39478170/chrome-webdriver-cannot-connect-to-the-service-chromedriver-exe-on-windows
            #chrome_options = webdriver.ChromeOptions()
            #chrome_options.add_argument("--no-startup-window")
            #driver = webdriver.Chrome("./chromedriver.exe")  # webdriver.PhantomJS('./phantomjs-2.1.1-windows/bin/phantomjs.exe') # http://zipsan.hatenablog.jp/entry/20150413/1428861548 # webdriver.Chrome('./chromedriver.exe') #
            try:
                _driver.get(url)
            except:
                print('webbrowser not responding, timeout happended...')
                # from timeout: Timed out receiving message from renderer: -0.019
                #   (Session info: chrome=56.0.2924.87)
                #   (Driver info: chromedriver=2.25.426923 (0390b88869384d6eb0d5d09729679f934aab9eed),platform=Windows NT 10.0.14393 x86_64)
                continue

            time.sleep(2)

            html = _driver.page_source.encode("utf-8")
            soup = BeautifulSoup(html, 'lxml')

            #driver.close()
            
        # reuters news specific parser
        try:
            header = soup.find('header', attrs={'class' : 'canvas-header'}).get_text().replace("\"","''").replace("\'","'")
        except: # no header title for this news
            continue 
        try:
            publish = soup.find('time')['datetime']
        except: # no timetamp for this news
            continue
        try:
            bodys = soup.find('article').findAll('p', attrs={'data-type' : 'text'})
        except:
            print('skipping body')
            continue

        if 'reuters' in _folder.lower():
            elasticsearch_index_name = 'Reuters'
        elif 'bloomberg' in _folder.lower():
            elasticsearch_index_name = 'Bloomberg'          
        elif 'ft.com' in _folder.lower():
            elasticsearch_index_name = 'FT'            
        elif 'street' in _folder.lower():
            elasticsearch_index_name = 'TheStreet'            
        elif 'marketwatch' in _folder.lower():
            elasticsearch_index_name = 'MarketWatch'            
        elif 'the-wall-street-journal' in _folder.lower():
            elasticsearch_index_name = 'WSJ'            
        elif 'new-york-times' in _folder.lower():
            elasticsearch_index_name = 'N.Y.Times'            
        elif 'cnbc' in _folder.lower():
            elasticsearch_index_name = 'CNBC'            
        elif 'forbes' in _folder.lower():
            elasticsearch_index_name = 'Forbes'            
        elif 'insider' in _folder.lower():
            elasticsearch_index_name = 'BusinessInsider'
        elif 'zacks' in _folder.lower():
            elasticsearch_index_name = 'Zacks'

        # concat the body texts and exclude the front (Reuters) copyright symbol
        body_str = []
        [body_str.append(line.get_text()) for line in bodys]
        body_str = ' '.join(body_str)   # .split('(Reuters) - ')[1]
        body_str = body_str.replace("\"","''").replace("\'","'").replace("’","'").replace("\’","'")
        print(body_str.encode('utf-8'),'\n')
        elasticsearch_index_name = elasticsearch_index_name.lower()
        type_name = 'html'
        fields_name = ['header', 'publish', 'body_text']
        # create JSON text to save
        json_str_list = []
        json_str = '{{"header":"{0}","publish":"{1}","body":"{2}"}}'.format(header,dpu.parse_any_date_str_to_iso_date(publish),r'{0}'.format(body_str))
        json_str = json_str.replace("\\'", "'")
        json_str_list.append(json_str)

        iou.create_folder_if_not_exist(_folder + 'html/')
        iou.create_folder_if_not_exist(_folder + 'text/')
        # move this processed html file into an archive.
        osu.rename_file(_folder + f, _folder + 'html/' + f)
        # save the extract text as a JSON file for later Elasticsearch indexing.
        json_file = _folder + 'text/' + f.replace('.html', '.json')
        #if not iou.file_exist(json_file):
        iou.write_to_json(json_file, json_str_list)
        #else:
        #    print('{0} is already existing and we skip saving this ... '.format(file))

        # insert into ElasticSearch Index            
#        PyBulkApi.immediate_insert(elasticsearch_index_name, type_name, fields_name, json_file, es)
        # move the json file into an archive
        json_arhieve = _folder + 'indexed/'        
        iou.create_folder_if_not_exist(json_arhieve)
        osu.rename_file(json_file, json_file.replace('/text/', '/indexed/'))

        #except:
        #    print('\n Coulnt parse {0} \n'.format(_folder + f))   # possibly an html from other domain site 

#if __name__=='__main__':    


#    send_gmail('メール送信テスト2','日本語のメールだよ★2')

#html = get_html_with_proxy('https://www.bloomberg.co.jp/')
#get_expire_calender_from_barchart('C:/Users/operator/Desktop/Futures - Expiration Calendar.html)
#get_active_month_from_barchart('C:/Users/operator/Desktop/Futures - Most Active Futures.html')
