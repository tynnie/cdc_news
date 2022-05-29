import re
import os
import time
import pandas as pd
import requests
from random import randint
from bs4 import BeautifulSoup
from user_agent import generate_user_agent

# webpage crawler setting
cdc_domain = 'https://www.cdc.gov.tw'
user_agent = generate_user_agent()
headers = {'user_agent': user_agent}

# file path setting
dir_path = os.path.dirname(os.path.realpath(__file__))
directory = ['/data/']
for d in directory:
    if not os.path.exists(dir_path + d):
        os.makedirs(dir_path + d)


def get_news_list(news_list_url):
    res = requests.get(news_list_url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    news_items = soup('div', class_='cbp-item')
    news_list = []
    for i in news_items:
        news_link = cdc_domain + i.a['href']
        news_list.append(news_link)

    return news_list


def get_news_content(news_item_url):
    res = requests.get(news_item_url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    news_content = soup('div', class_='news-v3-in')
    for i in news_content:
        try:
            title = i('h2', class_='con-title')[0].text.strip().split('\n')[0].strip()
        except (AttributeError, ValueError):
            title = ''

        try:
            date = i('div', class_='text-right')[0].text
            date = re.sub('發佈日期：|:', '', date)
        except (AttributeError, ValueError):
            date = ''

        paragraph = ''
        try:
            for text in i.find('div', class_='').text.split('\n'):
                if len(text) > 0 and '發佈日期' not in text:
                    paragraph += text.strip()
                    paragraph += '\n'
        except (AttributeError, ValueError):
            pass

        news_item = {'date': date, 'title': title, 'paragraph': paragraph}

    return news_item


def save_as_csvfile(items):
    df = pd.DataFrame(items)
    df['year_month'] = df['date'].apply(lambda x: '-'.join(x.split('-')[:2]))

    for i in list(set(df['year_month'].values)):
        df_year_month = df[df['year_month'] == i]
        df_year_month.to_csv(dir_path+f'/data/cdc_news_{i}.csv', index=False)


def main(page_start, page_end):
    targets = []
    try:
        for i in range(page_start, page_end):
            url = f'https://www.cdc.gov.tw/Bulletin/List/MmgtpeidAR5Ooai4-fgHzQ?page={i}'
            targets += get_news_list(url)

            if i%10 == 0:
                time.sleep(randint(1, 5))
            elif i%50 == 0:
                time.sleep(5)
            else:
                pass
    except requests.exceptions.HTTPError as err:
        print(err)

    news_items = []
    if len(targets) >= 1:
        try:
            for t in targets:
                news_items.append(get_news_content(t))
                time.sleep(1)
        except requests.exceptions.HTTPError as err:
            print(err)

    if len(news_items) >= 1:
        save_as_csvfile(news_items)


if __name__ == '__main__':
    START = 0  # the latest page
    END = 120  # the last page
    main(START, END)

