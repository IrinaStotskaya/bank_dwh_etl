# Дипломный проект "Разработка ETL процесса и создание витрины данных"

## Описание задачи
Разработать ETL процесс, получающий ежедневную выгрузку данных, загружающий ее в хранилище данных и ежедневно строящий отчет по мошенническим операциям.

## Источники данных
Сведения о картах, счетах и клиентах хранятся в СУБД PostgreSQL.
Ежедневно информационные системы выгружают три следующих файла:
1. Список транзакций за текущий день. Формат – CSV.
2. Список терминалов полным срезом. Формат – XLSX.
3. Список паспортов, включенных в «черный список» - с накоплением с начала месяца. Формат – XLSX.

## Построение отчета
По результатам загрузки ежедневно необходимо строить витрину отчетности по мошенническим операциям. Витрина строится накоплением, каждый новый отчет укладывается в эту же таблицу с новым report_dt.
Признаки мошеннических операций.
1. Совершение операции при просроченном или заблокированном паспорте.
2. Совершение операции при недействующем договоре.
3. Совершение операций в разных городах в течение одного часа.
4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

## Основные этапы 
1. Разработка хранилища данных, написание DDL команд для создания таблиц.
2. Извлечение данных из транзакционной базы данных и файлов от сторонних систем.
3. Трансформация. Выявление инкремента и измененных записей для загрузки в хранилище.
4. Загрузка целевых записей в хранилище.
5. Формирование витрины мошеннических операций.


## Файлы, используемые в проекте
main.ddl - DDL код для развертки необходимых таблиц в БД
main.py - скрипт ETL-процесса
main.cron - файл настройки cron

Проект оценен на отлично (115 баллов из 125)