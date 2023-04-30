from orator import DatabaseManager
import xml.etree.ElementTree as ET
from datetime import date, datetime
import requests

class Generator():
    db = None
    def __init__(self, domain, db_host, db_name, db_user, db_password, url_prefix, xml_location, baidu_token, baidu_url):
        self.domain = domain
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.url_prefix = url_prefix
        self.xml_location  = xml_location
        self.baidu_token = baidu_token
        self.baidu_url = baidu_url

    def get_orator_config(self):
        config = {
            'default': 'mysql',
            'mysql': {
                'driver': 'mysql',
                'host': self.db_host,
                'database': self.db_name,
                'user': self.db_user,
                'password': self.db_password,
                'prefix': ''
            }
        }

        return config
    
    def db_(self):
        if(self.db):
            return self.db
        else:
            config = self.get_orator_config()
            self.db = DatabaseManager(config=config)
            return self.db

    def get_latest_generated_id(self):
        db = self.db_()
        latest = db.table('sitemap_generated').order_by('article_id',  'desc').first()
        if latest:
            return latest['article_id']
        return 0
    
    def generate_xml(self):
        xml_data = self.load_xml()
        xml_root = xml_data.getroot()
        latest_id = self.get_latest_generated_id()
        date_now = str(date.today())

        sitemap_list = []

        for articles in self.db_().table('article').where('id', '>', latest_id).order_by('id', 'asc').limit(5).chunk(100):
            for item in articles:
                # delete fist one then append new one
                listed_url = xml_root.findall('url')
                if listed_url and len(listed_url) > 1000:
                    for idx in range(1000, len(listed_url)):
                        xml_root.remove(listed_url[idx])
                        print("delete xml : {}".format(idx))
                
                # creating new url object
                url_obj = ET.Element("url")
                url_obj.tail = "\n" 
                loc = ET.SubElement(url_obj, 'loc')
                lastmod = ET.SubElement(url_obj, 'lastmod')
                changefreq = ET.SubElement(url_obj, 'changefreq')
                
                loc.text = self.url_prefix.format(self.domain, item['id'])
                lastmod.text = date_now
                changefreq.text = 'always'

                # append new url object
                xml_root.insert(0, url_obj)
                print('append : {} {}'.format(item['id'], loc.text))
                sitemap_list.append({'article_id': item['id'], 'created_at': datetime.now()})
                self.push_to_baidu(loc.text)

        # ET.indent(xml_data, space="\t", level=0)
        xml_data.write(self.xml_location, encoding="utf-8")
        self.inset_to_sitemap_generated(sitemap_list)
        print('xml_updated.....')

    def load_xml(self):
        xml_data = ET.parse(self.xml_location)
        if not xml_data:
            print('please specify sitemap.xml location ith absolute path')
        else:
            return xml_data

    def inset_to_sitemap_generated(self, sitemap_list):
        self.db_().table('sitemap_generated').insert(sitemap_list)


    def push_to_baidu(self, item_url):
        response = requests.post(self.baidu_url, data=item_url)
        print(response.json())

if __name__ == "__main__":
    domain = 'http://www.longnanshi.com'
    db_host = '**************'
    db_name = '**********'
    db_user = '******'
    db_password = '**********'
    url_prefix = '{}/job/{}'
    # xml_location = 'sitemap.xml'
    xml_location = '/www/wwwroot/5hrc.com/public/sitemap.xml'
    baidu_token = 'TFXDBhUhHABKO2QS'
    baidu_url = 'http://data.zz.baidu.com/urls?site={}&token={}'.format(domain, baidu_token)


    generator = Generator(
        domain=domain,
        db_host=db_host,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        url_prefix=url_prefix,
        xml_location=xml_location,
        baidu_token=baidu_token,
        baidu_url=baidu_url,

    )

    generator.generate_xml()

    # breakpoint()