import kagglehub
import os
from collections import defaultdict
import multiprocessing as mp
from time import time


def mapper(line):
    if not line or ',' not in line or 'Event' in line:
        return []
    
    fields = line.strip().split(',')
    if len(fields) < 15:
        return []
    
    try:
        result = fields[3]
        white_elo = int(fields[6])
        black_elo = int(fields[7])
        termination = fields[14]
    except:
        return []
    
    output = []
    is_time_forfeit = "Time" in termination
    
    # Считаем ВСЕ игры
    output.append(('total_games', 1))

    # Игры с поражением по времени
    if is_time_forfeit:
        output.append(('total_time_forfeit', 1))
    
    # Апсет-статистика (только для побед)
    if result == '1-0' and (white_elo - black_elo) < -300:
        output.append(('upset_total', 1))
        if is_time_forfeit:
            output.append(('upset_time_forfeit', 1))

    elif result == '0-1' and (black_elo - white_elo) < -300:
        output.append(('upset_total', 1))
        if is_time_forfeit:
            output.append(('upset_time_forfeit', 1))
    
    return output


def process_chunk(chunk):
    mapped = []
    for line in chunk:
        mapped.extend(mapper(line))
    return mapped


def reducer(mapped_data):
    counts = defaultdict(int)
    for key, value in mapped_data:
        counts[key] += value
    return dict(counts)


def mapreduce_parallel(file_path, num_processes=4):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Убираем заголовок
    data_lines = lines[1:]
    
    chunk_size = len(data_lines) // num_processes
    chunks = [data_lines[i:i + chunk_size] for i in range(0, len(data_lines), chunk_size)]
    
    start_time = time()
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(process_chunk, chunks)
    
    all_mapped = []
    for result in results:
        all_mapped.extend(result)
    
    final_counts = reducer(all_mapped)
    print(f"Время обработки: {time() - start_time:.2f}с")
    
    return final_counts


def print_stats(counts):
    upset_total = counts.get('upset_total', 0)
    upset_tf = counts.get('upset_time_forfeit', 0)
    total_games = counts.get('total_games', 0)
    total_tf = counts.get('total_time_forfeit', 0)
    
    upset_percent = (upset_tf / upset_total * 100) if upset_total else 0
    total_percent = (total_tf / total_games * 100) if total_games else 0
    
    print(f"\nВсего игр: {total_games:,}")
    print(f"Апсет-побед (>300): {upset_total:,}")
    print(f"Апсеты с Time forfeit: {upset_tf:,} ({upset_percent:.1f}%)")
    print(f"Всего Time forfeit: {total_tf:,} ({total_percent:.1f}%)")
    
    if total_percent > 0:
        ratio = upset_percent / total_percent
        print(f"Отношение: {ratio:.2f}x")
        print("Гипотеза подтверждается" if ratio > 1 else "Гипотеза отвергается")


def main():
    path = kagglehub.dataset_download("arevel/chess-games")
    
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                csv_path = os.path.join(root, file)
                print(f"Обработка {file}...")
                counts = mapreduce_parallel(csv_path)
                print_stats(counts)
                return

if __name__ == "__main__":
    main()
