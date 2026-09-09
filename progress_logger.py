# de_framework/progress_logger.py

import time


class ProgressLogger:
    """Логирует прогресс длинной операции с ASCII прогресс-баром."""

    def __init__(self, total, desc='Processing', bar_width=30, log_every_pct=5):
        self.total = total
        self.desc = desc
        self.bar_width = bar_width
        self.log_every_pct = log_every_pct

        self.processed = 0
        self.last_logged_pct = -1
        self.start = time.time()

        print(f'📥 {desc}: starting {total:,} items', flush=True)

    def update(self, n=1):
        """Сообщает что обработали n элементов."""
        self.processed += n
        pct = self.processed * 100 // self.total

        if pct - self.last_logged_pct >= self.log_every_pct or self.processed >= self.total:
            self._log(pct)
            self.last_logged_pct = pct

    def finish(self):
        """Финальный лог."""
        elapsed = time.time() - self.start
        speed = self.processed / elapsed if elapsed > 0 else 0
        print(
            f'✅ {self.desc}: done! {self.processed:,} items in '
            f'{self._format_time(elapsed)} ({speed:,.0f}/sec)',
            flush=True
        )

    def fail(self, error):
        """Лог при ошибке."""
        elapsed = time.time() - self.start
        print(
            f'❌ {self.desc}: failed after {self._format_time(elapsed)} '
            f'at item {self.processed:,}: {error}',
            flush=True
        )

    def _log(self, pct):
        elapsed = time.time() - self.start
        speed = self.processed / elapsed if elapsed > 0 else 0
        eta = (self.total - self.processed) / speed if speed > 0 else 0

        bar = self._make_bar(pct)

        print(
            f'⏳ [{bar}] {pct:3d}% | '
            f'{self.processed:>10,}/{self.total:,} | '
            f'⚡ {speed:>7,.0f}/sec | '
            f'⏱ {self._format_time(elapsed):>8} | '
            f'⌛ ETA: {self._format_time(eta):>8}',
            flush=True
        )

    def _make_bar(self, pct):
        filled = pct * self.bar_width // 100
        return '█' * filled + '░' * (self.bar_width - filled)

    @staticmethod
    def _format_time(seconds):
        if seconds < 60:
            return f'{seconds:.0f}s'
        elif seconds < 3600:
            m, s = divmod(int(seconds), 60)
            return f'{m}m {s}s'
        else:
            h, rem = divmod(int(seconds), 3600)
            m, s = divmod(rem, 60)
            return f'{h}h {m}m {s}s'