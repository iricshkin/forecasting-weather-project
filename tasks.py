import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import BrokenThreadPool

import pandas

import my_logger
from api_client import YandexWeatherAPI
from data import CityData
from utils import CITIES

logger = my_logger.get_logger(__name__)


class DataFetchingTask:
    """Получает все данных через API Yandex.Weather"""
    @staticmethod
    def get_response(city_name: str) -> object:
        yw = YandexWeatherAPI()
        data = yw.get_forecasting(city_name)
        return data


class DataCalculationTask(multiprocessing.Process):
    """
    Вычисляет среднюю температуру за день,
    среднюю температуру за уканный промежуток времени,
    сумму часов без осадков за указанный промежуток времени.
    """
    def __init__(self, queue: multiprocessing.Queue) -> None:
        multiprocessing.Process.__init__(self)
        self.queue = queue

    @staticmethod
    def get_temp(city: str) -> dict:
        """Получает данные о погоде с 9 до 19 часов."""
        temp_list = []
        city_data = CityData.parse_obj(DataFetchingTask().get_response(city))
        city = city_data.geo_object.locality.name
        for forecast in city_data.forecasts:
            date = forecast.date
            weather = [(i.temp, i.condition) for i in forecast.hours]
            if not len(weather) < 24:
                temp_list.append(
                    {date: [weather[i] for i in range(9, 20)]}
                )
        return {city: temp_list}

    @staticmethod
    def calculate_condition(conditions: list) -> int:
        """Количество часов без осадков с 9 до 19 часов."""
        no_precipitation = ('clear', 'partly-cloud', 'cloudy', 'overcast')
        return len([item for item in conditions if item in no_precipitation])

    def calculate(self, city: str) -> dict:
        """Средняя температура и осадки с 9 до 19 часов."""
        all_weather, all_temp, all_cond = [], [], []
        city_data = self.get_temp(city)
        for i in city_data.values():
            for date in i:
                for weather in date.values():
                    temp, cond = list(zip(*weather))
                    av_day_temp = sum(temp) // len(temp)
                    all_temp.append(av_day_temp)
                    no_prec = self.calculate_condition(cond)
                    all_cond.append(no_prec)
                    current_date = list(date.keys())[0]
                    weather = {'average_temp': av_day_temp,
                               'no_precipitation': no_prec}
                    all_weather.append(
                        {'date': current_date, 'weather': weather}
                    )
        av_city_temp = sum(all_temp) // len(all_temp)
        city_name = list(city_data.keys())[0]
        city_weather = {
            'city': city_name,
            'all_weather': all_weather,
            'av_temp': av_city_temp,
            'no_precipitation': sum(all_cond),
            'rating': sum(all_temp, sum(all_cond))
        }
        logger.info(f' def average: получены данные - {city_weather}')
        return city_weather

    def run(self) -> None:
        with ThreadPoolExecutor() as executor:
            future = executor.map(self.calculate, CITIES)
            for item in future:
                try:
                    self.queue.put(item)
                    logger.info(f'executor отправил в очередь {item}')
                except ValueError as ve:
                    logger.error(f'{item} сгенерировано исключение {ve}')
                except BrokenThreadPool as btp:
                    logger.error(f'{item} сгенерировано исключение {btp}')


class DataAggregationTask(multiprocessing.Process):
    """
    Объединяет полученные данные и сохраняет результат в файл.
    """
    def __init__(self, queue: multiprocessing.Queue) -> None:
        multiprocessing.Process.__init__(self)
        self.queue = queue

    def run(self) -> None:
        data = []
        while not self.queue.empty():
            item = self.queue.get()
            logger.info(f'Из очереди получен элемент {item}')
            data.append(item)
            df = pandas.DataFrame.from_dict(data)
            df.to_csv('output.csv', mode='w', index=False)


class DataAnalyzingTask:
    """
    Анализирует полученные данные,
    делает вывод, какой из городов наиболее благоприятен для поездки.
    """
    @staticmethod
    def analyze() -> None:
        df = pandas.read_csv('output.csv', usecols=['city', 'rating'])
        result = df.sort_values(['rating'])
        idx = len(result) - 1
        city_to_go = result.iloc[idx]
        print(f'Наиболее благоприятный для поездок город {city_to_go["city"]}')
