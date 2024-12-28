## Запуск приложения локально

    python3 -m venv venv  # Создание окружения
    source venv/bin/activate  # Активация в Linux/macOS
    venv\Scripts\activate  # Активация в Windows
    pip install --upgrade pip
    
    pip install -r requirements.txt

    (venv) (base) dmitryloschinin@MacBookPro big_data_systems_2 % airflow webserver --port 8080
    (venv) (base) dmitryloschinin@MacBookPro big_data_systems_2 % airflow scheduler


python 3.12
admin
admin


airflow users create \
--username admin \   
--firstname Dmitry \
--role Admin \
--password admin   