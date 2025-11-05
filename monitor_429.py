import time
from pathlib import Path
from datetime import datetime

print('=' * 70)
print('SUPER TURBO MODE: 429 MONITOR')
print('=' * 70)
print('Config: 60 batch / 20 concurrent / 0.08s rate (~250 req/s)')
print('=' * 70)
print()

log_dir = Path('C:/TSIS_Data/trades_ticks_2019_2025/_batch_temp')
start_time = time.time()
check_count = 0
last_429_count = 0
trend_history = []

while True:
    time.sleep(15)
    check_count += 1

    errors_429 = 0
    total_logs = 0

    if log_dir.exists():
        for log_file in log_dir.glob('batch_*.log'):
            total_logs += 1
            try:
                content = log_file.read_text(encoding='utf-8', errors='ignore')
                errors_429 += content.count('429')
            except Exception:
                pass

    elapsed = int(time.time() - start_time)
    new_429 = errors_429 - last_429_count
    trend_history.append(new_429)

    if len(trend_history) > 10:
        trend_history.pop(0)

    avg_rate = sum(trend_history) / len(trend_history) if trend_history else 0
    status = '[ALERT]' if new_429 > 0 else '[OK]'

    print(f'[{datetime.now():%H:%M:%S}] Check #{check_count} ({elapsed}s) | Logs: {total_logs} | Total 429: {errors_429} | New: +{new_429} {status} | Avg: {avg_rate:.1f}/check')

    last_429_count = errors_429

    if elapsed > 300:
        print()
        print('5-minute monitoring complete. Process continues in background.')
        print(f'Final stats: {errors_429} total 429 errors')
        break
