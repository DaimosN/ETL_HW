https://www.kaggle.com/datasets/mohitbansal31s/dc-characters/data - ссылка на датасет

Команда для загрузки датасета в ydb
ydb --endpoint grpcs://{your_serverless} --database /{your_database} --sa-key-file "{path_your_key}" import file csv --path dc_universe_ --columns "id,PageID,Name,Universe,URL,Identity,Gender,Marital,Teams,Weight,Creators" --delimiter "," --skip-rows 1 --null-value "" --progress dc_characters_dataset_new2.csv
