import multiprocessing

from tasks import DataAggregationTask, DataAnalyzingTask, DataCalculationTask


def forecast_weather() -> None:
    """
    Анализ погодных условий по городам
    """
    queue = multiprocessing.Queue()
    calculate = DataCalculationTask(queue)
    to_csv = DataAggregationTask(queue)
    calculate.start()
    calculate.join()
    to_csv.start()
    to_csv.join()
    DataAnalyzingTask().analyze()


if __name__ == "__main__":
    forecast_weather()
