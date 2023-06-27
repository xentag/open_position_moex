import time
import pandas as pd
import os.path
import requests
import datetime
from datetime import timedelta, date, datetime


# https://www.moex.com/api/contract/OpenOptionService/{date_kotirovki[day]}/P/Si/json?_=1585229934380', Путты
# 'https://www.moex.com/api/contract/OpenOptionService/{date_kotirovki[day]}/C/Si/json?_=1585229934380', Каллы
# /iss/engines/[engine]/markets/[market]/boards/[board]/securities/[security]/candles код для загрузки свеченый за указаный период
def kotirovki_vigruzka(derevativ_name):  # Скачиваем историю котировок
    date_test = date.today() - timedelta(days=30)  # Вычитаем 30 дней чтобы не было повторов
    dannie_kotirovki = {"date_kotirovki": [], "close": [], "open_candel": [], "high": [], "low": [], "volume": []}  # Создаем словарь
    data_vigruka = '2013-01-01'  # изменить если хотите скачать более поздние данные 2013-01-01
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101Firefox/86.0"}  # Создаем загаловок
    try:
        if derevativ_name == 'RTS':  # Ссылка
            link = f'https://iss.moex.com/iss/engines/stock/markets/index/boards/RTSI/securities/RTSI/candles.json?limit=100&iss.meta=off&iss.only=history&from={date_test}&till={date.today()}&interval=24'
        elif derevativ_name == 'Si':  # Ссылка
            link = f'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM/candles.json?limit=100&iss.meta=off&iss.only=history&from={date_test}&till={date.today()}&interval=24'
        else:  # Ссылка
            link = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{derevativ_name}/candles.json?limit=100&iss.meta=off&iss.only=history&from={date_test}&till={date.today()}&interval=24'
        response_vigruzka = requests.get(link, headers=headers)  # Выгрузка файлов
        json_vigruzka = response_vigruzka.json()  # Делаем json
        ostanovka_vremeji = json_vigruzka['candles']['data'][-1][6]  # Чтобы остановить скачивание
        while True:
            if derevativ_name == 'RTS':  # Выгрузка файлов
                link = f'https://iss.moex.com/iss/engines/stock/markets/index/boards/RTSI/securities/RTSI/candles.json?limit=100&iss.meta=off&iss.only=history&from={data_vigruka}&till={date.today()}&interval=24'
            elif derevativ_name == 'Si':
                link = f'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM/candles.json?limit=100&iss.meta=off&iss.only=history&from={data_vigruka}&till={date.today()}&interval=24'
            else:
                link = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{derevativ_name}/candles.json?limit=100&iss.meta=off&iss.only=history&from={data_vigruka}&till={date.today()}&interval=24'
            response_vigruzka = requests.get(link, headers=headers)
            json_vigruzka = response_vigruzka.json()  # Делаем json
            for i in range(len(json_vigruzka['candles']['data'])):  # Перебор json файла
                dannie_kotirovki["open_candel"].append(json_vigruzka['candles']['data'][i][0])  # Добавляем данные в список
                dannie_kotirovki["close"].append(json_vigruzka['candles']['data'][i][1])  # Добавляем данные в список
                dannie_kotirovki["high"].append(json_vigruzka['candles']['data'][i][2])  # Добавляем данные в список
                dannie_kotirovki["low"].append(json_vigruzka['candles']['data'][i][3])  # Добавляем данные в список
                dannie_kotirovki["volume"].append(json_vigruzka['candles']['data'][i][5])  # Добавляем данные в список
                dannie_kotirovki["date_kotirovki"].append(json_vigruzka['candles']['data'][i][6])  # Добавляем данные в список
            if json_vigruzka['candles']['data'][i][6] != date.today():
                data_vigruka = json_vigruzka['candles']['data'][i][6]
                print(data_vigruka, json_vigruzka['candles']['data'][i][6])
            if data_vigruka == ostanovka_vremeji:
                for i in range(len(dannie_kotirovki['date_kotirovki'])):  # Перевод в datetime
                    dannie_kotirovki['date_kotirovki'][i] = datetime.strptime(str(dannie_kotirovki['date_kotirovki'][i]),
                         '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d')  # Чтобы убрать время и оставить только дату
                return dannie_kotirovki
    except Exception as e:
        print(f'Ошибка в выгрузке файла котировок{e}\nПробую еще')
        return kotirovki_vigruzka(derevativ_name=derevativ_name)  # Перезапуск цикла


def dowload_open_position(dannie_kotirovki, derevativ, derevativ_name):  # Скачиваем открытые позиции
    dannie_kotirovki['PhysicalLong'], dannie_kotirovki['PhysicalShort'], dannie_kotirovki['JuridicalLong'],\
        dannie_kotirovki['JuridicalShort'], dannie_kotirovki['objie_summa'], dannie_kotirovki["PhysicalLong_liza"], \
        dannie_kotirovki["PhysicalShort_liza"], dannie_kotirovki["PhysicalShort_liza"], dannie_kotirovki["JuridicalLong_liza"],\
        dannie_kotirovki["JuridicalShort_liza"], dannie_kotirovki["objie_summa_liza"], dannie_kotirovki["date_json_kotirovki"], \
        = [], [], [], [], [], [], [], [], [], [], [], []  # Добавляем столбцы в словарь
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101Firefox/66.0"}  # Создаем заголовок
    if derevativ_name.lower() == "sber":  # Условие тикер по другому на moex пишется
        derevativ_name = "SBRF"  # Присваем новое значение
    elif derevativ_name.lower() == "gazp":  # Условие тикер по другому на moex пишется
        derevativ_name = 'GAZR'  # Присваем новое значение
    for day in range(len(dannie_kotirovki["date_kotirovki"])):  # Цикл для выгрузки деривативов
        try:
            json_data = requests.get(
                f'https://www.moex.com/api/contract/OpenOptionService/{dannie_kotirovki["date_kotirovki"][day]}/{derevativ}/{derevativ_name}/json',
                headers=headers).json()  # Ссылка
            print(f'https://www.moex.com/api/contract/OpenOptionService/{dannie_kotirovki["date_kotirovki"][day]}/{derevativ}/{derevativ_name}/json')
        except Exception as exe:
            print(f'Ошибка {exe}')
            time.sleep(5)
            json_data = requests.get(
                f'https://www.moex.com/api/contract/OpenOptionService/{dannie_kotirovki["date_kotirovki"][day]}/{derevativ}/{derevativ_name}/json',
                headers=headers).json()  # Ссылка
        for item in range(int(len(json_data)) // 4):  # Перебор json в цикле
            dannie_kotirovki["date_json_kotirovki"].append(json_data[0]['Date'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["PhysicalLong"].append(json_data[0]['PhysicalLong'].replace(u'\xa0', u''))  # Лишние символы 
            dannie_kotirovki["PhysicalShort"].append(json_data[0]['PhysicalShort'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["JuridicalLong"].append(json_data[0]['JuridicalLong'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["JuridicalShort"].append(json_data[0]['JuridicalShort'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["objie_summa"].append(json_data[0]['Summary'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["PhysicalLong_liza"].append(json_data[3]['PhysicalLong'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["PhysicalShort_liza"].append(json_data[3]['PhysicalShort'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["JuridicalLong_liza"].append(json_data[3]['JuridicalLong'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["JuridicalShort_liza"].append(json_data[3]['JuridicalShort'].replace(u'\xa0', u''))  # Лишние символы
            dannie_kotirovki["objie_summa_liza"].append(json_data[3]['Summary'].replace(u'\xa0', u''))  # Лишние символы
            print(dannie_kotirovki["date_kotirovki"][day])
    df = pd.DataFrame(dannie_kotirovki)
    if os.path.isfile(f'data_save_{derevativ_name}_{derevativ}.csv') == True:  # Если есть файл уже то без заголовка сохраняем
        df.to_csv(f'data_save_{derevativ_name}_{derevativ}.csv', sep=',', index=False, header=False, mode='a',
                  date_format='%Y-%m-%d')
    else:  # Если нет то с заголовком сохраняем
        df.to_csv(f'data_save_{derevativ_name}_{derevativ}.csv', sep=',', index=True, header=True, mode='a',
                  date_format='%Y-%m-%d')


def main():
    aktiv = 'SBER'  # Менять чтобы скачать другое Si 'RTS' SBER GAZP MGNT AFLT VTBR LKOH
    dannie_kotirovki = kotirovki_vigruzka(derevativ_name=aktiv)  # Скачиваем открытые позиции
    dowload_open_position(dannie_kotirovki=dannie_kotirovki, derevativ="F", derevativ_name=aktiv)  # Фьючерсы скачивает буква F
    dowload_open_position(dannie_kotirovki=dannie_kotirovki, derevativ="P", derevativ_name=aktiv)  # Путы скачивает буква P
    dowload_open_position(dannie_kotirovki=dannie_kotirovki, derevativ="C", derevativ_name=aktiv)  # Каллы скачивает буква C


if __name__ == '__main__':
    main()

