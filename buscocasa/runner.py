import os
from time import sleep

import pandas as pd

from buscocasa.spiders.buscocasa_spider import BuscocasaCrawlSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


time_interval_hours = 24              #In Hours

essential_columns = ['REF', 'Title', 'Location', 'User_Id', 'User_Name', 'User_Phone', 'User_Email',
                     'User_Website', 'User_Address', 'Description', 'Price', 'Currency', 'image_urls',
                     'Views', 'Favorite', 'Date_Modified', 'Scrapped_Date', 'URL', 'UNPUBLISHED', 'SOLD_OUT',
                     'PAIS', 'POB', 'QUI', 'QUE', 'COM']

characteristics_columns = ['1 sol propietari', 'Accepten animals', 'Aire condicionat', 'Antena Satelit',
                           'Armaris Encastrats', 'Ascensor', 'Assolellat', 'Box Tancat', 'Calefacció',
                           'Cèntric', 'Cuina Americana', 'Cuina Equipada', 'Domòtica', 'Dutxa Hidromassatge',
                           'Jacuzzi', 'Jardí', 'Llar de Foc', 'Moblat', 'Nou per estrenar', 'Parquet',
                           'Piscina', 'Sistema Alarma', 'Terrassa', 'Traster', 'Vistes agradables']

dynamic_columns = []

def column_order(df):
    global dynamic_columns

    dynamic_columns = [col for col in df.columns if col not in essential_columns and col not in characteristics_columns]

    return essential_columns + characteristics_columns + dynamic_columns

def update_records(temp_df, old_df):
    global dynamic_columns

    available_records = temp_df.merge(old_df[['REF']], on=['REF'], how="left")

    sold_out = old_df.merge(temp_df[['REF']], how='outer', on=['REF'],
                            indicator=True).loc[lambda x: x['_merge'] == 'left_only']
    del sold_out['_merge']
    sold_out['SOLD_OUT'] = 1

    return available_records.append(sold_out)

def check_records(temp_filename):
    file_name = temp_filename.replace('temp_', '').replace(".json", '.xlsx')

    temp_df = pd.read_json(temp_filename)  # reading new temporary data into dataframe

    if not os.path.isfile(file_name):
        return temp_df, file_name

    old_df = pd.read_excel(file_name)  # reading old data into dataframe
    return update_records(temp_df, old_df), file_name


def remove_temp(temp_filename):
    try:
        os.remove(temp_filename)
    except Exception:
        pass

def file_formater(temp_filename):
    df, file_name = check_records(temp_filename)
    
    df['image_urls'] = df['image_urls'].apply(lambda x: ', '.join(x))

    df = df[column_order(df)]

    writer = pd.ExcelWriter(file_name, engine='xlsxwriter', options={'strings_to_urls': False})
    df.to_excel(writer, index=False)
    writer.close()

    remove_temp(temp_filename)


if __name__ == "__main__":
    temp_filename = "Data/temp_buscocasa.json"
    settings = get_project_settings()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'buscocasa.settings'
    settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
    settings.setmodule(settings_module_path, priority='project')

    while True:    
        process = CrawlerProcess(settings)
        process.crawl(BuscocasaCrawlSpider)
        process.start()
        file_formater(temp_filename)

        print(f'{"-"*10} Script will Restart in {time_interval_hours} Hours {"-"*10}')
        sleep(60*60*time_interval_hours)
