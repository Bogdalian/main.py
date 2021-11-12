import os
import time
import zipfile

from selenium import webdriver

PROXY_HOST = '192.168.2.94'  # rotating proxy
PROXY_PORT = 3128
PROXY_USER = 'b.bulatov'
PROXY_PASS = 'Hc92DwSB'
URL = 'http://auth.isiao.vpn/'
manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""
background_js = """
var config = {
        mode: "fixed_servers",
        rules: {
          singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
          },
          bypassList: ["localhost"]
        }
      };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);
""" % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)
def get_chromedriver(use_proxy=False, user_agent=None) -> object:
    path = os.path.dirname(os.path.abspath(__file__))
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = r'C:\Program Files\Google\Chrome Beta\Application\chrome.exe'
    if use_proxy:
        pluginfile = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(pluginfile, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        chrome_options.add_extension(pluginfile)
    if user_agent:
        chrome_options.add_argument('--user-agent=%s' % user_agent)
    driver = webdriver.Chrome(
        os.path.join(path, 'chromedriver'),
        chrome_options=chrome_options)
    return driver
path = 'Z:\Управление ИАС и СЦ\Проекты вне ГК\Модераторы Наш СПб\Иерархии\Загрузочные\неделя'
files = os.listdir(path)
files_xlsx = [i for i in files if i.endswith('.xlsx')]


def load_file(file_name):
    driver = get_chromedriver(use_proxy=True)
    driver.get(URL)
    driver.find_element_by_name('username').send_keys("data_collect_govern_adm")
    driver.find_element_by_name('password').send_keys("GSjM46OL")
    try:
        driver.find_element_by_xpath('//button[contains(text(), "Войти")]').click()
    except Exception:
        driver.find_element_by_xpath('//button[contains(text(), "Войти")]').click()
    driver.implicitly_wait(100)
    try:
        driver.get('http://isiao.vpn/monitorings/115/112/week')
    except Exception:
        driver.get('http://isiao.vpn/monitorings/115/112/week')
    driver.implicitly_wait(100)
    try:
        driver.find_element_by_xpath('//*[@id="scrollContent"]/div[1]/div[1]/div[2]/div/button[1]').click()
    except Exception:
        driver.find_element_by_xpath('//*[@id="scrollContent"]/div[1]/div[1]/div[2]/div/button[1]').click()
    driver.implicitly_wait(100)
    try:
        driver.find_element_by_xpath('//*[@id="menu-list-grow"]/ul/li[1]').click()
    except Exception:
        driver.find_element_by_xpath('//*[@id="menu-list-grow"]/ul/li[1]').click()
    driver.implicitly_wait(100)

    # Указать файл
    try:
        driver.find_element_by_xpath(
            '//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[1]/div/label/span[1]/input').send_keys(
            f'Z:\Управление ИАС и СЦ\Проекты вне ГК\Модераторы Наш СПб\Иерархии\Загрузочные\неделя\ {file_name}')
    except Exception:
        driver.find_element_by_xpath(
            '//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[1]/div/label/span[1]/input').send_keys(
            f'Z:\Управление ИАС и СЦ\Проекты вне ГК\Модераторы Наш СПб\Иерархии\Загрузочные\неделя\ {file_name}')

    # Загрузить файл
    try:
        driver.find_element_by_xpath('//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[2]/div/button').click()
    except Exception:
        driver.find_element_by_xpath('//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[2]/div/button').click()

    try:
        driver.find_element_by_xpath('//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[2]/div/div/button').click()
    except Exception:
        driver.find_element_by_xpath('//*[@id="platform-layout"]/div[2]/div[3]/div/div[2]/div[2]/div/div/button').click()





    # # Опубликовать
    # try:
    #     driver.find_element_by_xpath('//*[@id="scrollContent"]/div[1]/div[1]/div[2]/div/button[3]').click()
    # except Exception:
    #     driver.find_element_by_xpath('//*[@id="scrollContent"]/div[1]/div[1]/div[2]/div/button[3]').click()
    # time.sleep(4)
    # driver.close()

#load_file('Загрузочный 3 неделя 2021 (c 11.01.2021).xlsx')