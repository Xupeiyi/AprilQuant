#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import time

import requests
import pandas as pd
from bs4 import BeautifulSoup

URL_PATTERN = 'https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/{}.phtml'


# In[2]:


def _extract_row_data(row_tag) -> (str, list):
    """
    提取某一行的名称和数据。
    row_tag: html中的 <tr>...</tr> 标签
    """
    squares = [tag.text for tag in row_tag.find_all('td')]
    title, row_data = squares[0], squares[1:]
    return title, row_data

def extract_table_data(table_tag) -> pd.DataFrame:
    """
    提取表格中的数据。
    table_tag: html中的 <table>...</table> 标签
    """
    row_tags = table_tag.find('tbody').find_all('tr')
    table_data = dict()
    for row_tag in row_tags:
        title, row_data = _extract_row_data(row_tag)
        table_data[title] = row_data
        
    table_data = pd.DataFrame(table_data)
    return table_data


# In[3]:


def _convert_capital_stock_quantity(quantity):
    return int(float(quantity.replace('万股', '')) * 1e4) if '万股' in quantity else quantity

def _clean_title(title):
    return title.replace('·', '').replace('(历史记录)', '').strip()

def clean_structure_table(structure_table):
    structure_table.columns = list(map(_clean_title, structure_table.columns))
    structure_table = structure_table.drop(columns=['股本结构图', '流通股'])
    structure_table = structure_table.applymap(_convert_capital_stock_quantity)
    structure_table.replace('--', 0, inplace=True)
    return structure_table


# In[18]:


def download_stock_structure(code)->pd.DataFrame:
    print(f'downloading: {code}')
    url = URL_PATTERN.format(code)
    r = requests.get(url)
    html = r.content.decode('gbk')
    soup = BeautifulSoup(html, features="lxml")

    table_tags = soup.find_all('table', id=re.compile('StockStructureNewTable[0-9]+')) 
    stock_structure = pd.concat(map(extract_table_data, table_tags))
    stock_structure = clean_structure_table(stock_structure)
    stock_structure['股票代码'] = code
    print(f'downloaded: {code}')
    time.sleep(5)
    return stock_structure


# In[15]:


stock_list = pd.read_csv('科创板.csv',dtype='object')['A股代码'].to_list()
structure_list = []
# with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
#     res = executor.map(download_stock_structure, stock_list)
# stock_structure_data = pd.concat(list(res))


# In[19]:


for code in stock_list:
    structure_list.append(download_stock_structure(code))


# In[20]:


data = pd.concat(structure_list)


# In[21]:


data


# In[22]:


data.to_csv('科创板股本结构.csv', index=False, encoding='utf8')

