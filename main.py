from datetime import timedelta, date, datetime
import pandas as pd
import os.path
import requests
import datetime
import json
import numpy as np
import csv
import time


# https://www.moex.com/api/contract/OpenOptionService/{date_kotirovki[day]}/P/Si/json?_=1585229934380', Путты
# 'https://www.moex.com/api/contract/OpenOptionService/{date_kotirovki[day]}/C/Si/json?_=1585229934380', Каллы
# /iss/engines/[engine]/markets/[market]/boards/[board]/securities/[security]/candles код для загрузки свеченый за указаный период
def kotirovki_vigruzka():
    date_test = date.today() - datetime.timedelta(days=30)
    date_kotirovki, close = [], []
    data_vigruka = '2013-01-01'  # изменить если хотите скачать более поздние данные 2013-01-01
    if os.path.isfile('data_save.csv') == True:
        dt = datetime.datetime.strptime((pd.read_csv('data_save.csv')['date_kotirovki'][0]), "%Y-%m-%d %H:%M:%S")
        dt_data_vigruka = datetime.datetime.strptime(data_vigruka, "%Y-%m-%d")
        if dt.month != dt_data_vigruka.month or dt.year != dt_data_vigruka.year:  # Условие чтобы данные заново скачать если месяц и год не совпадает
            os.remove('data_save.csv')
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101Firefox/86.0"}
    try:
        response_vigruzka = requests.get(
            f'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM/candles.json?limit=100&iss.meta=off&iss.only=history&from={date_test}&till={date.today()}&interval=24',
            headers=headers)
        json_vigruzka = response_vigruzka.json()
        ostanovka_vremeji = json_vigruzka['candles']['data'][-1][6]
        while True:
            response_vigruzka = requests.get(
                f'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM/candles.json?limit=100&iss.meta=off&iss.only=history&from={data_vigruka}&till={date.today()}&interval=24',
                headers=headers)
            json_vigruzka = response_vigruzka.json()
            for i in range(len(json_vigruzka['candles']['data'])):
                close.append(json_vigruzka['candles']['data'][i][1])
                date_kotirovki.append(json_vigruzka['candles']['data'][i][6])
            if json_vigruzka['candles']['data'][i][6] != date.today():
                data_vigruka = json_vigruzka['candles']['data'][i][6]
                print(data_vigruka, json_vigruzka['candles']['data'][i][6])
            if data_vigruka == ostanovka_vremeji:
                for i in range(len(date_kotirovki)):  # Убрать часы с даты
                    date_kotirovki[i] = date_kotirovki[i].split(' ')[0]
                return (date_kotirovki, close)
    except Exception as e:
        print(f'Ошибка в выгрузке файла котировок{e}\nПробую еще')
        kotirovki_vigruzka()


def proverka(date_kotirovki, close):
    now = datetime.datetime.now()
    today9_45am = now.replace(hour=21, minute=45, second=1, microsecond=0)
    if date.today().weekday() != (5) and date.today().weekday() != (6):
        if now < today9_45am:
            del date_kotirovki[-1]  # удаляю последний элемент в списке
            del close[-1]  # удаляю последний элемент в списке
    if os.path.isfile(
            'data_save.csv') == True:  # Проверяет файл если файл сохрением дат или нет, чтобы заново не качать данные
        df = pd.DataFrame(dict(date_kotirovki=date_kotirovki, close=close))
        df.to_csv('perebor_now_close.csv', sep=',', index=False, header=True, mode='w')
        # perebor_date_kotirovki = set(pd.read_csv('data_save.csv')['date_kotirovki'])  # тут set потому что даты это уникальные значения, а котировки могут повторяться
        # date_kotirovki = [x for x in date_kotirovki if x not in perebor_date_kotirovki]
        date_kotirovki = pd.read_csv("data_save.csv")['date_kotirovki']
        perebor_now_close = pd.read_csv('perebor_now_close.csv')['close'].tolist()
        perebor_close = pd.read_csv('data_save.csv')['close'].tolist()
        vihitanie = (len(perebor_now_close) - len(perebor_close)) * -1
        if vihitanie == 1:
            close = []
        elif vihitanie != 0:
            close = close[vihitanie:]
        else:
            close = []
    return date_kotirovki, close


def dowload_open_position(date_kotirovki, close, derevativ, derevativ_name):
    date_json_kotirovki, PhysicalLong, PhysicalShort, JuridicalLong, JuridicalShort, putov_summa = [], [], [], [], [], []
    PhysicalLong_liza, PhysicalShort_liza, JuridicalLong_liza, JuridicalShort_liza, putov_summa_liza = [], [], [], [], []
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101Firefox/66.0"}
    for day in range(len(date_kotirovki)):  # Цикл для выгрузки деривативов
        json_data = requests.get(
            f'https://www.moex.com/api/contract/OpenOptionService/{date_kotirovki[day]}/{derevativ}/{derevativ_name}/json?_=1585229934380',
            headers=headers).json()
        for item in range(int(len(json_data)) // 4):
            date_json_kotirovki.append(json_data[0]['Date'].replace(u'\xa0', u''))
            PhysicalLong.append(json_data[0]['PhysicalLong'].replace(u'\xa0', u''))
            PhysicalShort.append(json_data[0]['PhysicalShort'].replace(u'\xa0', u''))
            JuridicalLong.append(json_data[0]['JuridicalLong'].replace(u'\xa0', u''))
            JuridicalShort.append(json_data[0]['JuridicalShort'].replace(u'\xa0', u''))
            putov_summa.append(json_data[0]['Summary'].replace(u'\xa0', u''))
            PhysicalLong_liza.append(json_data[3]['PhysicalLong'].replace(u'\xa0', u''))
            PhysicalShort_liza.append(json_data[3]['PhysicalShort'].replace(u'\xa0', u''))
            JuridicalLong_liza.append(json_data[3]['JuridicalLong'].replace(u'\xa0', u''))
            JuridicalShort_liza.append(json_data[3]['JuridicalShort'].replace(u'\xa0', u''))
            putov_summa_liza.append(json_data[3]['Summary'].replace(u'\xa0', u''))
            print(date_kotirovki[day])
            print(close[day])
    data_frame = dict(date_kotirovki=date_kotirovki, close=close, PhysicalLong=PhysicalLong,
                      PhysicalShort=PhysicalShort,
                      JuridicalLong=JuridicalLong, JuridicalShort=JuridicalShort, putov_summa=putov_summa,
                      PhysicalLong_liza=PhysicalLong_liza,
                      PhysicalShort_liza=PhysicalShort_liza, JuridicalLong_liza=JuridicalLong_liza,
                      JuridicalShort_liza=JuridicalShort_liza, putov_summa_liza=putov_summa_liza)
    df = pd.DataFrame(data_frame)
    if os.path.isfile(f'data_save_{derevativ_name}_{derevativ}.csv') == True:
        df.to_csv(f'data_save_{derevativ_name}_{derevativ}.csv', sep=',', index=False, header=False, mode='a',
                  date_format='%Y-%m-%d')
    else:
        df.to_csv(f'data_save_{derevativ_name}_{derevativ}.csv', sep=',', index=True, header=True, mode='a',
                  date_format='%Y-%m-%d')


def main():
    date_kotirovki, close = kotirovki_vigruzka()
    date_kotirovki, close = proverka(date_kotirovki, close)
    dowload_open_position(date_kotirovki=date_kotirovki, close=close, derevativ="P",
                          derevativ_name='Si')  # Путы скачивает буква P Si
    dowload_open_position(date_kotirovki=date_kotirovki, close=close, derevativ="C",
                          derevativ_name='Si')  # Каллы скачивает буква C
    dowload_open_position(date_kotirovki=date_kotirovki, close=close, derevativ="F",
                          derevativ_name='Si')  # Фьючерсы скачивает буква F


if __name__ == '__main__':
    main()
