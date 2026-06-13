# Подготовка виртуальной машины

## Склонируйте репозиторий

Склонируйте репозиторий проекта:

```
git clone https://github.com/yandex-praktikum/mle-project-sprint-4-v001.git
```

## Активируйте виртуальное окружение

Используйте то же самое виртуальное окружение, что и созданное для работы с уроками. Если его не существует, то его следует создать.

Создать новое виртуальное окружение можно командой:

```
python3 -m venv env_recsys_start
```

После его инициализации следующей командой

```
. env_recsys_start/bin/activate
```

установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```

### Скачайте файлы с данными

Для начала работы понадобится три файла с данными:
- [tracks.parquet](https://storage.yandexcloud.net/mle-data/ym/tracks.parquet)
- [catalog_names.parquet](https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet)
- [interactions.parquet](https://storage.yandexcloud.net/mle-data/ym/interactions.parquet)
 
Скачайте их в директорию локального репозитория. Для удобства вы можете воспользоваться командой wget:

```
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet

wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet

wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Запустите Jupyter Lab

Запустите Jupyter Lab в командной строке

```
jupyter lab --ip=0.0.0.0 --no-browser
```

# Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Изначально, это шаблон. Используйте его для выполнения первой части проекта.


# Сервис рекомендаций

Код сервиса рекомендаций находится в файле `recommendations_service.py`.

Для работы сервиса необходимы файлы:

- `data/personal_als.parquet` — персональные offline-рекомендации;
- `data/top_popular.parquet` — рекомендации для новых пользователей;
- `data/similar_items.parquet` — похожие треки для online-рекомендаций.

их можно загрузить с помощью `/home/nikita/projects/mle-project-sprint-4-v001/load_parquet_from_s3.ipynb`


Установите зависимости:

```bash
pip install -r requirements.txt
```

Запустите сервис из корневой директории проекта:

```bash
uvicorn recommendations_service:app --host 0.0.0.0 --port 8000
```


Сервис предоставляет следующие endpoints:

- `POST /events` — сохраняет новое событие пользователя;
- `POST /recommendations_offline` — возвращает offline-рекомендации;
- `POST /recommendations_online` — возвращает online-рекомендации;
- `POST /recommendations` — возвращает смешанные online- и offline-рекомендации.

При наличии online-истории рекомендации чередуются:

```text
online[0], offline[0], online[1], offline[1], ...
```

После смешивания удаляются дубликаты, а результат ограничивается параметром `k`.

# Инструкции для тестирования сервиса

Код для тестирования сервиса находится в файле `test_service.py`.

Перед запуском тестирования убедитесь, что сервис работает:

```bash
uvicorn recommendations_service:app --host 0.0.0.0 --port 8000
```

В другом терминале запустите тестовый скрипт:

```bash
python test_service.py
```

Скрипт проверяет следующие сценарии:

1. Пользователь без персональных рекомендаций получает популярные треки.
2. Пользователь с персональными рекомендациями без online-истории получает offline-рекомендации.
3. Пользователь с персональными рекомендациями и online-историей получает смешанную выдачу.

Результаты одновременно выводятся в терминал и автоматически сохраняются в файл:

```text
test_service.log
```

По умолчанию тестирование выполняется для сервиса по адресу `http://127.0.0.1:8000`.

