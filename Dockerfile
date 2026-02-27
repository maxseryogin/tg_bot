FROM python:3.12-slim

# ffmpeg нужен для yt-dlp (конвертация аудио в mp3)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        wget \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала устанавливаем зависимости (кэширование слоёв Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Обновляем yt-dlp сразу после установки (всегда нужна свежая версия)
RUN yt-dlp --update-to stable || true

# Копируем остальные файлы проекта
COPY . .

CMD ["python", "telegram_bot.py"]