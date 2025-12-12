from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from time import time


class ChessAnalysisMR(MRJob):
    def mapper(self, _, line):
        fields = list(csv.reader([line]))[0]
            
        try:
            result = fields[3]
            white_elo = int(fields[6])
            black_elo = int(fields[7])
            termination = fields[13]
        except:
            return
        
        is_time_forfeit = "Time forfeit" in termination
        
        # Считаем ВСЕ игры
        yield 'total_games', 1

        # Игры с поражением по времени
        if is_time_forfeit:
            yield 'total_time_forfeit', 1
        
        # Апсет-статистика (только для побед)
        if result == '1-0' and (white_elo - black_elo) < -300:
            yield 'upset_total', 1
            if is_time_forfeit:
                yield 'upset_time_forfeit', 1
        elif result == '0-1' and (black_elo - white_elo) < -300:
            yield 'upset_total', 1
            if is_time_forfeit:
                yield 'upset_time_forfeit', 1
    

    def reducer(self, key, values):
        yield key, sum(values)
    

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


def print_stats(results_dict):
    upset_total = results_dict.get('upset_total', 0)
    upset_tf = results_dict.get('upset_time_forfeit', 0)
    total_games = results_dict.get('total_games', 0)
    total_tf = results_dict.get('total_time_forfeit', 0)
    
    upset_percent = (upset_tf / upset_total * 100) if upset_total else 0
    total_percent = (total_tf / total_games * 100) if total_games else 0
    
    print(f"Всего игр: {total_games:,}")
    print(f"Апсет-побед: {upset_total:,}")
    print(f"Апсеты с Time forfeit: {upset_tf:,} ({upset_percent:.1f}%)")
    print(f"Всего Time forfeit: {total_tf:,} ({total_percent:.1f}%)")
    
    if total_percent > 0:
        ratio = upset_percent / total_percent
        print(f"Отношение: {ratio:.2f}x")
        print("Гипотеза подтверждается" if ratio > 1 else "Гипотеза отвергается")


if __name__ == '__main__':
    start_time = time()

    # Создаем экземпляр задачи
    mr_job = ChessAnalysisMR()
    
    # Запускаем задачу и собираем результаты
    results_dict = {}
    with mr_job.make_runner() as runner:
        runner.run()
        
        # Парсим вывод
        for key, value in mr_job.parse_output(runner.cat_output()):
            results_dict[key] = value
    
    # Выводим статистику
    print_stats(results_dict)

    print(f"Время обработки: {time() - start_time:.2f}с")
