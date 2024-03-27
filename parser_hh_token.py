import requests
import time
from concurrent.futures import ThreadPoolExecutor
from fake_useragent import UserAgent
import os
import json
import logging
import re
import pandas as pd

#Класс для API HH (Получение токена OAuth 2.0)
class OAuthTokenManager:
    '''
    Класс управляет жизненным циклом OAuth-токенов, обеспечивая их актуальность и обновление при необходимости,
    чтобы приложение всегда имело доступ к данным на сайте hh.ru
    '''
    def __init__(self, client_id, client_secret, access_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.expires_at = 0
        self.refresh_token = None

    def read_token_info(self):
        """
        Метод для чтения информации о токенах из файла 'token_info.txt'.
        При наличии файла с данными токенов, он считывает их и сохраняет в атрибуты объекта.
        """
        try:
            with open('token_info.txt', 'r') as file:
                lines = file.readlines()
                if len(lines) >= 3:
                    self.access_token = lines[0].strip()
                    self.expires_at = float(lines[1].strip())
                    self.refresh_token = lines[2].strip()
        except FileNotFoundError:
            pass

    def save_token_info(self):
        """
        Метод для сохранения информации о токенах в файл 'token_info.txt'.
        Записывает текущие токены и срок их действия в указанный файл.
        """
        with open('token_info.txt', 'w') as file:
            file.write(self.access_token + '\n')
            file.write(str(self.expires_at) + '\n')
            file.write(self.refresh_token + '\n')

    def is_token_valid(self):
        """
        Метод для проверки действительности токена
        """
        if self.access_token:
            if time.time() < self.expires_at:
                # Выполните тестовый запрос к API, например, к /me или /user
                test_url = 'https://api.hh.ru/me'
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                }

                response = requests.get(test_url, headers=headers)
                if response.status_code == 200:
                    # Токен действителен, возвращаем True
                    return True
            test_url = 'https://api.hh.ru/me'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
            }

            response = requests.get(test_url, headers=headers)
            if response.status_code == 200:
                # Токен действителен, возвращаем True
                return True

            # Токен недействителен или его нет, возвращаем False
            return False

    def get_oauth_token(self):
        """
        Метод для получения OAuth-токена.
        Если у текущего токена еще не истек срок действия, он возвращается.
        В противном случае, если у нас есть refresh_token, происходит попытка обновления токена.
        Если нет refresh_token, выполняется запрос для получения новой пары токенов.
        """
        self.read_token_info()

        if self.is_token_valid():
            # Токен действителен, возвращаем его
            return self.access_token

        if self.refresh_token:
            refresh_token_url = 'https://hh.ru/oauth/token'
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
            }
            # Запрос на обновление токена
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            response = requests.post(refresh_token_url, data=data, headers=headers)

            if response.status_code == 200:
                # Обработка успешного ответа и сохранение нового токена
                response_data = response.json()
                self.access_token = response_data.get('access_token')
                expires_in = response_data.get('expires_in')

                if self.access_token and expires_in:
                    self.expires_at = time.time() + expires_in
                    self.save_token_info()
                    return self.access_token
                else:
                    logging.error("Не удалось получить токен OAuth. Данные ответа неполные.")
            else:
                logging.error(f"Не удалось получить токен OAuth. Код состояния: {response.status_code}")
                logging.error(response.text)
        else:
            # Если у нас нет refresh_token, то делаем запрос для получения новой пары токенов
            initial_token_url = 'https://hh.ru/oauth/token'
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
            }
            # Запрос на получение новой пары токенов
            response = requests.post(initial_token_url, data=data)

            if response.status_code == 200:
                # Обработка успешного ответа и сохранение новой пары токенов
                response_data = response.json()
                self.access_token = response_data.get('access_token')
                expires_in = response_data.get('expires_in')
                self.refresh_token = response_data.get('refresh_token')

                if self.access_token and expires_in and self.refresh_token:
                    self.expires_at = time.time() + expires_in
                    self.save_token_info()
                    return self.access_token
                else:
                    logging.error("Не удалось получить токен OAuth. Данные ответа неполные.")
            else:
                logging.error(f"Не удалось получить токен OAuth. Код состояния: {response.status_code}")
                logging.error(response.text)

class HHDataFetcher:
    '''
    Класс HHDataFetcher предназначен для извлечениея данных о вакансиях с сайта hh.ru (HeadHunter).
    Функциональность:
    Авторизация и получение доступа к данным.
    Извлечение данных о вакансиях с использованием API hh.ru. Вы можете указать профессии и регионы для поиска.
    Параллельная обработка вакансий: многопоточность для обработки нескольких вакансий одновременно.
    Логирование: Позволяет отслеживать процесс выполнения и обнаруживать возможные ошибки.
    Переработка токенов: Класс управляет авторизацией, обновлением и получением OAuth-токенов для доступа к данным.
    '''
    def __init__(self, client_id=None, client_secret=None, professional_roles=None, regions_list=None, access_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.professional_roles = professional_roles
        self.regions_list = regions_list
        self.ua = UserAgent()
        self.access_token = access_token

        # Настройка логирования
        logging.basicConfig(filename='parser_hh.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

        # Проверяем, существует ли директория для данных пагинации, и если нет, то создаем её
        directory = './docs/pagination'
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Проверяем, существует ли директория для данных о вакансиях, и если нет, то создаем её
        directory = './docs/vacancies'
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Получение и инициализация регионов
        self.init_regions()

    def init_regions(self):
        # Получение регионов и инициализация
        sub_regions = self.get_region(filter_regions='113', sub_region=False)
        self.regions_list = [region['id'] for region in sub_regions]
        self.regions_list.sort()

    def get_region(self, filter_regions='113', sub_region=False):
        '''
        Получает список регионов с заданным 'parent_id' и, при необходимости, более вложенных регионов.

        :param filter_regions: Идентификатор родительского региона для фильтрации.
        :param sub_region: Если True, возвращает более вложенные регионы.
        :return: Список регионов в виде словарей.
        '''

        # URL для получения дерева всех регионов API hh.ru
        url = 'https://api.hh.ru/areas'

        # Выполняем GET-запрос
        response = requests.get(url)

        # Проверяем успешность запроса
        if response.status_code == 200:
            # Декодируем JSON-ответ
            regions_data = response.json()

            # Возвращаем список регионов с заданным 'parent_id'
            regions = [reg for reg in regions_data[0]['areas'] if reg['parent_id'] == filter_regions]

            # Получаем более вложенные регионы, если sub_region=True
            if sub_region:
                sub_regions = []
                for reg in regions:
                    sub_regions.extend(reg.get('areas', []))
                return sub_regions
            else:
                return regions
        else:
            logging.error(f"Ошибка при запросе: {response.status_code}")
            return []

    def get_page(self, page=0, professional_role=None, area=None):
        '''
        Метод выполняет GET-запрос к API HeadHunter для получения данных о вакансиях.

        :param page: Номер страницы поиска.
        :param professional_role: Профессиональная роль, по которой осуществляется поиск вакансий.
        :param area: Регион (географическая область) для поиска вакансий.

        :return: Данные о вакансиях в формате текста.
        '''
        try:
            # Определяем заголовки для HTTP-запроса, включая авторизацию через токен доступа.
            headers = {'User-Agent': self.ua.random}
            if self.access_token is not None:
                headers = {
                    'Authorization': f'Bearer {self.access_token}', 'User-Agent': self.ua.random,
                }

            # Определяем параметры для GET-запроса, такие как номер страницы и количество вакансий на странице.
            params = {
                'page': page,  # Номер страницы поиска
                'per_page': 100,  # Количество вакансий на одной странице
            }

            # Если задана профессия, добавляем ее в параметры запроса.
            if professional_role is not None:
                params['professional_role'] = professional_role

            # Если задан регион, также добавляем его в параметры запроса.
            if area is not None:
                params['area'] = area

            # Выполняем GET-запрос к API HeadHunter с указанными параметрами и заголовками.
            req = requests.get('https://api.hh.ru/vacancies', params=params, headers=headers)

            # Получаем текстовое содержимое ответа.
            data = req.content.decode()

            # Закрываем соединение с сервером.
            req.close()

            # Возвращаем данные о вакансиях.
            return data
        except Exception as e:
            # Если возникает ошибка, выводим сообщение об ошибке с описанием исключения.
            logging.error(f"Ошибка при запросе: {str(e)}")
            print(f"Ошибка: {str(e)}")

    def process_vacancy(self, v):
        '''
        Метод выполняет обработку данных о вакансии и сохраняет их в файл формата JSON.

        :param v: Словарь, представляющий информацию о вакансии.

        Метод проверяет, существует ли файл с идентификатором вакансии.
        Если файл уже существует, то вакансия считается обработанной и парсинг пропускается.
        В противном случае, создается новый заголовок (с случайным User-Agent),
        и выполняется GET-запрос к URL вакансии для получения данных.
        Полученные данные записываются в JSON-файл с идентификатором вакансии в названии файла.
        Метод также записывает информацию о успешной обработке вакансии в лог-файл.
        В случае возникновения ошибки, метод записывает информацию об ошибке в лог-файл.

        '''
        try:
            # Проверяем, существует ли файл с ID вакансии
            fileName = './docs/vacancies/{}.json'.format(v['id'])
            if os.path.exists(fileName):
                # Проверяем размер файла
                file_size = os.path.getsize(fileName)  # Размер файла в байтах
                if file_size > 1060:  # 1 килобайт = 1024 байта
                    logging.info(f"Вакансия {v['id']} уже обработана и размер файла больше 1 кБ. Пропуск...")
                    return  # Пропускаем парсинг вакансии, если файл уже существует и размер больше 1 кБ

            # Создаем заголовок (headers) с новым случайным User-Agent
            headers = {'User-Agent': self.ua.random}
            req = requests.get(v['url'], headers=headers)
            data = req.content.decode()
            req.close()

            # Создаем файл в формате JSON с идентификатором вакансии в названии
            # Записываем в него ответ запроса и закрываем файл
            fileName = './docs/vacancies/{}.json'.format(v['id'])
            with open(fileName, mode='w', encoding='utf8') as f:
                f.write(data)

            # Небольшая задержка перед следующей операцией
            time.sleep(0.4)

            # Логируем успешную обработку вакансии
            logging.info(f"Вакансия {v['id']} успешно обработана")
        except Exception as e:
            # Логируем ошибку обработки вакансии
            logging.error(f"Ошибка при обработке вакансии {v['id']}: {str(e)}")

    def fetch_data(self):
        '''
        Метод для получения данных о вакансиях из различных профессий и регионов.

        Метод начинает процесс сбора данных о вакансиях, перебирая профессии и регионы,
        заданные в объекте. Для каждой профессиональной роли и региона выполняются запросы
        к API и обработка данных. Метод обрабатывает страницы с вакансиями, сохраняя их в отдельные файлы,
        и логирует успешное завершение этапов.

        '''
        try:
            # Создаем пул потоков с ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as executor:
                for p_r in self.professional_roles:
                    for reg in self.regions_list:
                        page = 0
                        while True:
                            response = self.get_page(page, p_r, reg)
                            #print(response)

                            jsObj = json.loads(response)

                            # Сохраняем файлы в папку pagination для каждой комбинации профессии и региона
                            nextFileName = f'./docs/pagination/{p_r}_{reg}_{page}.json'
                            with open(nextFileName, mode='w', encoding='utf8') as f:
                                f.write(json.dumps(jsObj, ensure_ascii=False))

                            # Проверка на последнюю страницу, если вакансий меньше 5000
                            if (jsObj['pages'] - page) <= 1:
                                break

                            # Обработка данных с текущей страницы
                            #for v in jsObj['items']:
                            #    executor.submit(self.process_vacancy, v)

                            page += 1

                            time.sleep(0.3)

                        # Логируем завершение обработки вакансий для данной профессии и региона
                        logging.info(f'Данных для профессии {p_r} и региона {reg} больше нет')
                        print(f'Данных для профессии {p_r} и региона {reg} больше нет')
        except KeyError as e:
            # Логируем ошибку, если ключ 'items' отсутствует в JSON данных
            logging.error(f"KeyError: 'items' не найден в данных JSON: {str(e)}")

    def fetch_vacancy_details(self):
        '''
        Метод для получения детальной информации о вакансиях.

        Метод проходит по списку файлов со списком вакансий в папке "pagination"
        и для каждой вакансии в каждом файле выполняет запрос к API для получения
        детальной информации о вакансии. Полученные данные записываются в отдельные JSON-файлы
        в папке "vacancies".

        '''
        for fl in os.listdir('./docs/pagination'):
            try:
                # Открываем файл, читаем его содержимое и автоматически закрываем файл после использования
                with open('./docs/pagination/{}'.format(fl), encoding='utf8') as f:
                    jsonText = f.read()

                # Преобразуем полученный текст в объект справочника
                jsonObj = json.loads(jsonText)

                # Получаем и проходимся по непосредственно списку вакансий
                for v in jsonObj['items']:
                    # Обращаемся к API и получаем детальную информацию по конкретной вакансии
                    try:
                        req = requests.get(v['url'])

                        # Проверяем статус код ответа
                        if req.status_code == 200:
                            # Обработка успешного ответа
                            data = req.content.decode()

                            # Создаем файл в формате JSON с идентификатором вакансии в качестве названия
                            # Записываем в него ответ запроса и закрываем файл
                            fileName = './docs/vacancies/{}.json'.format(v['id'])
                            with open(fileName, mode='w', encoding='utf8') as f:
                                f.write(data)

                            # Необязательная задержка, чтобы не нагружать сервисы HH, оставим 1 секунду
                            time.sleep(0.4)

                            logging.info(f'Вакансия {v["id"]} успешно обработана')

                        else:
                            # Обработка других статус кодов, например, 404, 401, и т.д.
                            if req.status_code == 429:
                                # Ошибка, связанная с капчей
                                captcha_url = req.json().get('captcha_url')
                                logging.warning(
                                    f'Ошибка 429 (капча) при обработке вакансии {v["id"]}. URL капчи: {captcha_url}')
                                # Далее можно перейти на страницу с капчей и решить её вручную
                            else:
                                # Другие обработки ошибок
                                logging.error(f'Ошибка {req.status_code} при обработке вакансии {v["id"]}: {req.text}')

                    except Exception as e:
                        logging.error(f'Ошибка при запросе к вакансии {v["id"]}: {str(e)}')

            except Exception as e:
                logging.error(f'Ошибка при обработке файла {fl}: {str(e)}')

        logging.info('Вакансии собраны')

    def process_pagination_files(self):
        '''
        Метод для обработки файлов с данными пагинации.

        Метод перебирает файлы, содержащие данные пагинации, и обрабатывает вакансии, сохраненные в этих файлах.
        Для каждого файла выполняется чтение данных, декодирование JSON и последующая обработка вакансий с использованием
        пула потоков. Метод также логирует возможные ошибки при обработке файлов и завершение процесса.

        '''
        for fl in os.listdir('./docs/pagination'):
            try:
                # Открываем файл, читаем его содержимое и автоматически закрываем файл после использования
                with open(f'./docs/pagination/{fl}', encoding='utf8') as f:
                    jsonText = f.read()

                # Преобразуем полученный текст в объект справочника
                jsonObj = json.loads(jsonText)

                # Создаем пул потоков с ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=1) as executor:
                    for v in jsonObj['items']:
                        # Добавляем задачи на обработку вакансий в пул потоков
                        executor.submit(self.process_vacancy, v)

            except json.JSONDecodeError as e:
                # Логируем ошибку декодирования JSON
                logging.error(f"JSON ошибка декодирования для файла {fl}: {str(e)}")
            except KeyError:
                # Логируем ошибку KeyError, если ключ 'items' отсутствует в JSON данных
                logging.error(f"KeyError: 'items' не найден в данных JSON для файла {fl}")
                continue

        # Логируем успешное завершение обработки файлов пагинации
        logging.info('Вакансии собраны')

class HHDataParser:
    '''
    Преобразование данных, полученных с сайта hh.ru в более удобный формат (CSV)
    '''
    def __init__(self):
        # Настройки логирования
        logging.basicConfig(filename='parser_hh.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def clean_text(self, text):
        '''
        Метод принимает текст и удаляет из него нежелательные символы, оставляя только буквы, цифры, пробелы и
         запятые. Он используется для очистки текста, полученного из JSON-файлов.
        '''
        # Удалить все символы, кроме букв, цифр, пробелов и запятых
        clean_text = re.sub('<.*?>', '', text)
        clean_text = re.sub(r'[^\w\s,^a-zA-Zа-яА-Я]', ' ', clean_text)
        return clean_text

    def extract_nested_value(self, data, keys):
        '''
        Метод принимает словарь data и список keys, который представляет путь к значению во вложенных словарях.
        Метод извлекает значение, указанное в keys, из вложенных словарей. Если какой-либо ключ не существует или
        возникает ошибка, метод возвращает None.
        '''
        try:
            for key in keys:
                data = data[key]
            return data
        except (KeyError, TypeError):
            return None

    def parse_json_files(self, input_folder, output_csv):
        '''
        Метод выполняет парсинг JSON-файлов, находящихся в указанной папке input_folder. Он извлекает и фильтрует
        данные из JSON-файлов, а затем сохраняет их в CSV-файл с именем output_csv. Если нет данных для записи,
        он записывает предупреждение в лог.
        '''
        # Создаем список для хранения словарей данных
        data_list = []

        # Проходимся по всем файлам JSON в указанной папке
        for filename in os.listdir(input_folder):
            if filename.endswith('.json'):
                file_path = os.path.join(input_folder, filename)

                # Открываем файл и считываем его содержимое с использованием кодировки 'utf-8-sig'
                try:
                    with open(file_path, 'r', encoding='utf-8-sig') as file:
                        data = json.load(file)
                        # Проверяем, что данные не пусты
                        if data:
                            # Фильтруем и выбираем нужные поля
                            filtered_data = {
                                'id': data['id'],
                                'is_premium': data['premium'],
                                'billing_type_id': self.extract_nested_value(data, ['billing_type', 'id']),
                                'billing_type_name': self.extract_nested_value(data, ['billing_type', 'name']),
                                'relations': data['relations'],
                                'name': data['name'],
                                'insider_interview': data['insider_interview'],
                                'is_response_letter_required': data['response_letter_required'],
                                'area_id': self.extract_nested_value(data, ['area', 'id']),
                                'area_name': self.extract_nested_value(data, ['area', 'name']),
                                'area_url': self.extract_nested_value(data, ['area', 'url']),
                                'salary': data['salary'],
                                'type_id': data['type']['id'],
                                'type_name': data['type']['name'],
                                'address': data['address'],
                                'allow_messages': data['allow_messages'],
                                'experience_id': self.extract_nested_value(data, ['experience', 'id']),
                                'experience_name': self.extract_nested_value(data, ['experience', 'name']),
                                'schedule_id': self.extract_nested_value(data, ['schedule', 'id']),
                                'schedule_name': self.extract_nested_value(data, ['schedule', 'name']),
                                'employment_id': self.extract_nested_value(data, ['employment', 'id']),
                                'employment_name': self.extract_nested_value(data, ['employment', 'name']),
                                'department': data['department'],
                                'contacts': data['contacts'],
                                'description': self.clean_text(data['description']),
                                'key_skills': [skill['name'] for skill in data['key_skills']],
                                'is_accept_handicapped': data['accept_handicapped'],
                                'is_accept_kids': data['accept_kids'],
                                'is_archived': data['archived'],
                                'response_url': data['response_url'],
                                'specializations': [spec['name'] for spec in data['specializations']],
                                'professional_roles': [role['name'] for role in data['professional_roles']],
                                'code': data['code'],
                                'is_hidden': data['hidden'],
                                'is_quick_responses_allowed': data['quick_responses_allowed'],
                                'driver_license_types': data['driver_license_types'],
                                'is_accept_incomplete_resumes': data['accept_incomplete_resumes'],
                                'employer_id': data['employer']['id'],
                                'employer_name': data['employer']['name'],
                                'employer_url': data['employer']['url'],
                                'employer_alternate_url': data['employer']['alternate_url'],
                                'employer_logo_original': data['employer']['logo_urls']['original'],
                                'employer_logo_240': data['employer']['logo_urls']['240'],
                                'employer_logo_90': data['employer']['logo_urls']['90'],
                                'vacancies_url': data['employer']['vacancies_url'],
                                'is_accredited_it_employer': data['employer']['accredited_it_employer'],
                                'is_trusted_employer': data['employer']['trusted'],
                                'published_at': data['published_at'],
                                'created_at': data['created_at'],
                                'initial_created_at': data['initial_created_at'],
                                'negotiations_url': data['negotiations_url'],
                                'suitable_resumes_url': data['suitable_resumes_url'],
                                'apply_alternate_url': data['apply_alternate_url'],
                                'has_test': data['has_test'],
                                'test': data['test'],
                                'alternate_url': data['alternate_url'],
                                'working_days': data['working_days'],
                                'working_time_intervals': data['working_time_intervals'],
                                'working_time_modes': data['working_time_modes'],
                                'is_accept_temporary': data['accept_temporary'],
                                'languages': data['languages']
                            }
                            data_list.append(filtered_data)

                except Exception as e:
                    logging.error(f"Ошибка при парсинге и записи данных из файла {filename}: {str(e)}")

        # Проверяем, что есть данные для записи в CSV
        if data_list:
            # Создаем DataFrame из списка словарей
            df = pd.DataFrame(data_list)

            # Сохраняем данные в CSV файл
            df.to_csv(output_csv, index=False, encoding='utf-8', sep='|')
            logging.info(f"Данные успешно записаны в {output_csv}")
        else:
            logging.warning("Нет данных для записи в CSV")

# Инициализация и запуск получения данных
if __name__ == "__main__":

    # Список кодов проффесий для настройки фильтрации вакансий
    professional_roles = [
        '156', '160', '10', '12', '150', '25', '165', '34', '36', '73', '155', '96', '164', '104', '157', '107', '112',
        '113', '148', '114', '116', '121', '124', '125', '126',
    ]


    # Client ID, Client Secret и access_token.
    # практика плохая, но  раелизовать по другому пока руки не дошли

    client_id = 'K5DN3PJIVMJS4Q82627QFGBVMLG02GOQBL4JIL8OLH0LROQRV0RRGFE4LKP5QG11'
    client_secret = 'V6OKEDJEIQL9IKEA1D8FTD30G8I3QF7DR411H3A5ST6G0T2UQ4T3NSIA5ATUV33R'
    access_token = 'APPLRDFPI8C61C9066NQ9QNJA12GIGG9RV2DI8784AOILMQUAI84NP7215EQGENE'

    # Создание OAuthTokenManager
    token_manager = OAuthTokenManager(client_id, client_secret, access_token)
    access_token = token_manager.get_oauth_token()

    # Загрузка данных
    data_fetcher = HHDataFetcher(client_id=client_id, client_secret=client_secret, professional_roles=professional_roles,
                                 regions_list=None, access_token=access_token)
   # data_fetcher.process_pagination_files()
   # data_fetcher.fetch_data()
    data_fetcher.fetch_vacancy_details()

    # Парсинг данных из Json в csv
    data_parser = HHDataParser()
    input_folder = 'D:/Pyton/pythonProject/pythonProject2/docs/vacancies/'  # Укажите путь к вашей папке с файлами JSON
    output_csv = 'vacancies_hh.csv'  # Укажите имя выходного CSV файла
    data_parser.parse_json_files(input_folder, output_csv)


