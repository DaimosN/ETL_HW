Итоговое задание 3 модуля


1) Собрать и запустить сервисы
docker-compose up -d --build

2) Проверка статуса контейнеров:
docker-compose ps

3) Запуск генератора данных
docker-compose run --rm data-generator

4) Проверка данных в MongoDB:
docker-compose exec mongodb mongo -u admin -p admin --eval '
    use analytics;
    db.UserSessions.countDocuments();
    db.ProductPriceHistory.countDocuments();
'

5) Активировать DAG data_replication в веб-интерфейсе
