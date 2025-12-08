import os, signal, atexit, re
import json, time, random
from base64 import b64decode
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from PIL import Image
from playwright.sync_api import (
    sync_playwright,
    Page,
    TimeoutError as PWTimeoutError,
    Error as PWError,
)

# ВХОДНОЙ ФАЙЛ С ССЫЛКАМИ
INPUT_FILE = Path("new_ads/АВТОСАЛОН 05.12.xlsx")  # Имя Excel/CSV-файла с ссылками на объявления

INPUT_SHEET = None  # Имя листа в Excel; None = использовать все листы
URL_COLUMN = None   # Имя колонки со ссылками; None = искать ссылки во всех колонках

# ПАПКИ И ОСНОВНЫЕ ВЫХОДНЫЕ ФАЙЛЫ
OUT_DIR = Path("avito_phones_playwright")  # Рабочая директория парсера
OUT_DIR.mkdir(exist_ok=True)    # mkdir - создание папки, если её нет
IMG_DIR = (OUT_DIR / "phones")  # Сюда будут сохраняться PNG с номерами (если SAVE_DATA_URI = False  (То что не провряли давно и не используется))
IMG_DIR.mkdir(exist_ok=True)
DEBUG_DIR = OUT_DIR / "debug"   # Сюда складываем скриншоты и html проблемных объявлений
DEBUG_DIR.mkdir(exist_ok=True)

OUT_JSON = (OUT_DIR / "phones" / "phones_map.json")          # Основной результат: {url: data:image... или тег __SKIP_*__}
PENDING_JSON = (OUT_DIR / "phones" / "pending_review.json")  # Ссылки «на модерации» и с лимитом контактов (в разработке на будущее)
SAVE_DATA_URI = (True)                                       # True = сохраняем data:image в JSON; False = сохраняем PNG в IMG_DIR
HEADLESS = False                                             # False = браузер виден (можно логиниться руками)

# ОБЪЁМ И ПАРАЛЛЕЛЬНОСТЬ
TEST_TOTAL = 766  # Максимум объявлений за один запуск (обрежется по списку ссылок)
CONCURRENCY = 3   # Количество одновременно открытых вкладок браузера (2–3 оптимально)


# БАЗОВЫЕ ТАЙМАУТЫ
CLICK_DELAY = 8       # Базовая задержка в секундах перед ожиданием появления номера телефона
NAV_TIMEOUT = 90_000  # Таймаут загрузки страницы, мс (90 секунд)


# НАСТРОЙКИ ПРОКСИ
USE_PROXY = False                # True = использовать прокси, False = напрямую
PROXY_HOST = "mproxy.site"       # Хост прокси-сервера
PROXY_PORT = 17518               # Порт прокси-сервера
PROXY_LOGIN = "YT4aBK"           # Логин для авторизации на прокси
PROXY_PASSWORD = "nUg2UTut9UMU"  # Пароль для авторизации на прокси

# ПОВЕДЕНИЕ (МЕДЛЕННЕЕ И ЕСТЕСТВЕНЕЕ)
PAGE_DELAY_BETWEEN_BATCHES = (2.4, 5.2, )    # Пауза между партиями ссылок (раньше была (2.0, 4.0))
NAV_STAGGER_BETWEEN_TABS = (0.45, 1.35, )    # Пауза перед открытием КАЖДОЙ вкладки (чтобы не стартовали все разом)
POST_NAV_IDLE = (0.45, 1.05,)                # Небольшая «заминка» после загрузки страницы перед действиями
BATCH_CONCURRENCY_JITTER = (True)            # Иногда работаем 2 вкладками вместо 3 для естественности
CLOSE_STAGGER_BETWEEN_TABS = (0.25, 0.75, )  # Вкладки закрываем с небольшой случайной паузой


# USER-AGENT браузера
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ЧЕЛОВЕЧНОСТЬ / АНТИБАН-ПОВЕДЕНИЕ
HUMAN = {
    "pre_page_warmup_scrolls": (1, 3, ),      # Сколько раз «прогрелись» скроллом после открытия страницы
    "scroll_step_px": (250, 900),             # Диапазон шага скролла в пикселях
    "scroll_pause_s": (0.18, 0.75),           # Пауза между скроллами
    "hover_pause_s": (0.14, 0.42),            # Пауза при наведении на элементы
    "pre_click_pause_s": (0.10, 0.28),        # Короткая пауза перед кликом
    "post_click_pause_s": (0.12, 0.32),       # Пауза сразу после клика
    "mouse_wiggle_px": (4, 12),               # Амплитуда «подёргивания» мыши
    "mouse_wiggle_steps": (2, 5),             # Сколько шагов «подёргиваний» мыши
    "between_actions_pause": (0.10, 0.30, ),  # Пауза между действиями (скролл, клик, наведение)
    "click_delay_jitter": (
        CLICK_DELAY * 0.9,
        CLICK_DELAY * 1.25,
    ),  # Случайная задержка после клика по телефону (min и max)
    "randomize_selectors": True,  # Флаг случайного изменения порядка селекторов
}


# Теги в phones_map.json при пропусках
TAG_NO_CALLS = "__SKIP_NO_CALLS__"        # Объявление «без звонков» / только сообщения
TAG_UNAVAILABLE = "__SKIP_UNAVAILABLE__"  # Объявление закрыто/удалено/недоступно
TAG_ON_REVIEW = "__SKIP_ON_REVIEW__"      # Объявление ещё на модерации
TAG_LIMIT = "__SKIP_LIMIT__"              # Закончился лимит показа контактов на аккаунте


# ХЕЛПЕРЫ

def human_sleep(a: float, b: float):
    '''
    Приостанавливает выполнение на случайное количество секунд в диапазоне [a, b].
    Используется для имитации человеческих пауз и предотвращения блокировок!
    '''
    time.sleep(random.uniform(a, b))


def human_pause_jitter():
    '''
    Короткая пауза между действиями на основе настройки HUMAN["between_actions_pause"].
    Добавляет естественности поведению скрипта.
    '''
    human_sleep(*HUMAN["between_actions_pause"])


def human_scroll_jitter(page: Page, count: int | None = None):
    '''
    Имитирует человеческий скроллинг страницы.
    Выполняет случайное количество скроллов со случайным шагом и направлением.
    page: Playwright Page объект
    count: Количество скроллов
    '''
    if count is None:
        count = random.randint(*HUMAN["pre_page_warmup_scrolls"]) # Случайное количество скролов
    try:
        height = page.evaluate("() => document.body.scrollHeight") or 3000
        for _ in range(count):
            step = random.randint(*HUMAN["scroll_step_px"])
            direction = 1 if random.random() > 0.25 else -1
            y = max(0, min(height, page.evaluate("() => window.scrollY") + step * direction))
            page.evaluate("y => window.scrollTo({top: y, behavior: 'smooth'})", y)  # Плавный скролл через JavaScript
            human_sleep(*HUMAN["scroll_pause_s"])
    except Exception:
        pass


def human_wiggle_mouse(page: Page, x: float, y: float):
    '''
    Имитирует мелкие случайные движения мыши вокруг указанных координат.
    Добавляет реалистичности наведению мыши.
    '''
    steps = random.randint(*HUMAN["mouse_wiggle_steps"])  # Шаги подергиваний
    amp = random.randint(*HUMAN["mouse_wiggle_px"])  # Амплитуда подергиваний
    for _ in range(steps):
        dx = random.randint(-amp, amp)  # Смещения x и y
        dy = random.randint(-amp, amp)
        try:
            page.mouse.move(x + dx, y + dy)
        except Exception:
            pass
        human_pause_jitter()  # Пауза между движениями


def human_hover(page: Page, el):
    '''
    Имитирует человеческое наведение мыши на элемент.
    Вычисляет центр элемента, добавляет случайное смещение и вибрацию мыши.
    el: Элемент для наведения
    '''
    try:
        box = el.bounding_box()  # Получение координат и размеров элемента
        if not box:
            return
        cx = box["x"] + box["width"] * random.uniform(0.35, 0.65)  # Корды x, y в пределах элемента
        cy = box["y"] + box["height"] * random.uniform(0.35, 0.65)
        page.mouse.move(cx, cy)
        human_wiggle_mouse(page, cx, cy)
        human_sleep(*HUMAN["hover_pause_s"])
    except Exception:
        pass


def safe_get_content(page: Page) -> str:
    '''
    Безопасно получает HTML-содержимое страницы с одной попыткой повторения.
    Return: HTML-код страницы или пустая строка при ошибке
    '''
    for _ in range(2):
        try:
            return page.content()
        except PWError:  # Обработка ошибок Playwright
            time.sleep(1)
    return ""



def is_captcha_or_block(page: Page) -> bool:
    '''
    Проверка на капчу. 
    Return: True если обнаружены признаки блокировки или капчи
    '''
    try:
        url = page.url.lower()  # Получение URL
    except PWError:
        url = ""
    html = safe_get_content(page).lower()  # Получение HTML
    return (
        "captcha" in url or 
        "firewall" in url or
        "доступ с вашего ip-адреса временно ограничен" in html
    )


def close_city_or_cookie_modals(page: Page):
    '''
    Закрывает всплывающие модальные окна (укажите город; куки; уведомления).
    Пытается найти и кликнуть на кнопки закрытия по различным селекторам.
    '''
    selectors = [
        "button[aria-label='Закрыть']",
        "button[data-marker='modal-close']",
        "button[class*='close']",
        "button:has-text('Понятно')",
        "button:has-text('Хорошо')",
        "button:has-text('Согласен')",
        "button:has-text('Принять')",
    ]
    for sel in selectors:  # Цикл по всем селекторам
        try:
            for b in page.query_selector_all(sel):  # Поиск всех элементов по селектору
                try:
                    if b.is_visible():  # Проверка видимости элемента
                        human_hover(page, b)
                        b.click()
                        human_sleep(0.25, 0.7)
                except Exception:
                    continue
        except Exception:
            continue


def close_login_modal_if_exists(page: Page) -> bool:
    '''
    Пытается закрыть окно авторизации, если оно появилось.
    Return: True если модальное окно было найдено и попытка закрытия выполнена
    '''
    selectors_modal = [
        "[data-marker='login-form']",
        "[data-marker='registration-form']",
        "div[class*='modal'][class*='auth']",
        "div[class*='modal'] form[action*='login']",
    ]  # Селекторы авторизации
    close_selectors = [
        "button[aria-label='Закрыть']",
        "button[data-marker='modal-close']",
        "button[class*='close']",
        "button[type='button']",
    ]  # Селекторы закрытия
    for sel in selectors_modal:
        try:
            modals = page.query_selector_all(sel)  # Поиск всех модальных окон по селектору
        except PWError:
            continue
        for m in modals:
            if not m.is_visible():
                continue
            for btn_sel in close_selectors:
                btn = m.query_selector(btn_sel)  # Поиск кнопки закрытия внутри модального окна
                if btn and btn.is_enabled():
                    try:
                        human_hover(page, btn)
                        human_sleep(*HUMAN["pre_click_pause_s"])
                        btn.click()
                        human_sleep(*HUMAN["post_click_pause_s"])
                        print("Модалка авторизации закрыта, объявление пропущено.")
                        return True
                    except Exception:
                        pass
            print("Модалка авторизации не закрывается — объявление пропускаем.")
            return True
    return False


def save_phone_png_from_data_uri(data_uri: str, file_stem: str) -> str | None:
    '''
    Сохраняет изображение телефона из data:image URI в PNG файл.
    Args:
        data_uri: Строка data:image с изображением
        file_stem: Имя файла без расширения
    Return: Путь к сохраненному файлу или None при ошибке
    '''
    try:
        _, b64_data = data_uri.split(",", 1)  # Разделение data:image URI и получение base64 данных
        raw = b64decode(b64_data)             # Декодирование base64 в бинарные данные
        image = Image.open(BytesIO(raw)).convert("RGB")  # Создание изображения из бинарных данных
        file_name = f"{file_stem}.png"
        out_path = IMG_DIR / file_name  # Путь к файлу
        image.save(out_path)
        print(f"PNG сохранён: {out_path}")
        return str(out_path)
    except Exception as e:
        print(f"Ошибка при сохранении PNG: {e}")
        return None


def get_avito_id_from_url(url: str) -> str:
    '''
    Извлекает ID объявления из URL Avito.
    Arg: url объявления Avito
    Return: ID объявления или timestamp если ID не найден
    '''
    m = re.search(r"(\d{7,})", url)
    return m.group(1) if m else str(int(time.time()))


def try_click(page: Page, el) -> bool:
    '''
    Пытается кликнуть на элемент различными способами.
    Return: True если клик выполнен успешно
    '''
    try:
        el.scroll_into_view_if_needed()  # Прокрутка страницы к элементу
    except Exception:
        pass
    human_hover(page, el)
    human_sleep(*HUMAN["pre_click_pause_s"])
    try:
        el.click()
        human_sleep(*HUMAN["post_click_pause_s"])
        return True
    except Exception:
        try:  # Попытка альтернативного клика через JavaScript
            box = el.bounding_box() or {}
            if box:
                page.mouse.move(box.get("x", 0) + 6, box.get("y", 0) + 6)  # Перемещение мыши к элементу со смещением
                human_sleep(*HUMAN["pre_click_pause_s"])
            page.evaluate("(e)=>e.click()", el)  # Клик через JS
            human_sleep(*HUMAN["post_click_pause_s"])
            return True
        except Exception:
            return False


def is_limit_contacts_modal(page: Page) -> bool:
    '''
    Проверяет наличие модального окна о лимите контактов.
    Return: True если обнаружено сообщение о лимите контактов
    '''
    html = safe_get_content(page).lower()
    if "закончился лимит" in html and "просмотр контактов" in html:
        return True
    try:
        loc = page.locator("text=Купить контакты").first
        if loc.is_visible():
            return True
    except Exception:
        pass
    return False


# КЛАССИФИКАЦИЯ СТРАНИЦЫ ОБЪЯВЛЕНИЯ
NO_CALLS_MARKERS = [
    "без звонков",
    "пользователь предпочитает сообщения",
]
MODERATION_MARKERS = [
    "оно ещё на проверке",
    "объявление на проверке",
    "объявление ещё на проверке",
]
UNAVAILABLE_MARKERS = [
    "объявление не посмотреть",
    "объявление снято с продажи",
    "объявление удалено",
    "объявление закрыто",
    "объявление больше не доступно",
]


def classify_ad_status(page: Page) -> str:
    '''
    Определяет статус объявления по содержимому страницы.
    Return: Строка с статусом: 'ok' | 'no_calls' | 'on_review' | 'unavailable' | 'blocked' | 'limit'
    '''
    if is_captcha_or_block(page):
        return "blocked"

    html = safe_get_content(page).lower()

    # Проверка лимита контактов
    if is_limit_contacts_modal(page):
        return "limit"
    
    # Проверка модерации
    if any(m in html for m in MODERATION_MARKERS):
        return "on_review"
    
    # Проверка доступности
    if any(m in html for m in UNAVAILABLE_MARKERS):
        return "unavailable"
    
    # Проверка режима "без звонков"
    if any(m in html for m in NO_CALLS_MARKERS):
        return "no_calls"

    try:
        if page.locator("text=Без звонков").first.is_visible():
            return "no_calls"
    except Exception:
        pass

    return "ok"  # Возвращаем 'ok', если проблем не обнаружено


def read_urls_from_excel_or_csv(path: Path, sheet=None, url_column=None) -> list[str]:
    '''
    Читает URL объявлений из Excel или CSV файла.
    Args:
        path: Путь к файлу
        sheet: Имя листа Excel (None для всех листов)
        url_column: Имя колонки с URL (None для поиска во всех колонках)
    Return: Список уникальных URL
    '''
    url_re = re.compile(r'https?://(?:www\.)?avito\.ru/[^\s"]+')  # Регулярка для поиска URL Avito
    urls: list[str] = []

    if path.suffix.lower() in {".xlsx", ".xls"}:
        xls = pd.ExcelFile(path)  # Создание объекта Excel
        sheets = [sheet] if sheet is not None else xls.sheet_names  # Определение листов для обработки
        for sh in sheets:
            df = xls.parse(sh, dtype=str)  # Чтение листа как DataFrame
            if url_column and url_column in df.columns:
                col = df[url_column].dropna().astype(str)  # Получение колонки и удаление пустых значений
                urls.extend(col.tolist())  # Добавление значений в список URL
            else:  # Если колонка не указана
                for col in df.columns:
                    s = df[col].dropna().astype(str)  # Получение колонки как строки
                    for val in s:
                        urls.extend(url_re.findall(val))  # Поиск URL в значении
    elif path.suffix.lower() in {".csv", ".txt"}:
        df = pd.read_csv(path, dtype=str, sep=None, engine="python")
        if url_column and url_column in df.columns:
            col = df[url_column].dropna().astype(str)
            urls.extend(col.tolist())
        else:
            for col in df.columns:
                s = df[col].dropna().astype(str)
                for val in s:
                    urls.extend(url_re.findall(val))
    else:
        raise ValueError("Поддерживаются .xlsx/.xls/.csv/.txt")

    cleaned = []
    seen = set()  # Инициализация множества для отслеживания уникальных URL
    for u in urls:
        u = u.strip()
        if not u.startswith("http"):
            u = urljoin("https://www.avito.ru", u)
        u = u.split("#", 1)[0]  # Удаление якорей
        u = u.split("?", 1)[0]  # Удаление параметров запроса
        if u not in seen:  # Проверка уникальности URL
            seen.add(u)
            cleaned.append(u)
    return cleaned


def atomic_write_json(path: Path, data):
    '''
    Атомарно записывает данные в JSON файл с использованием временного файла.
    Arg: data: Данные для записи
    '''
    tmp = path.with_suffix(path.suffix + f".tmp_{int(time.time()*1000)}_{random.randint(1000,9999)}")  # Создание уникального имени временного файла
    payload = json.dumps(data, ensure_ascii=False, indent=2)  # Преобразование данных в JSON строку
    tmp.write_text(payload, encoding="utf-8") 
    attempts, delay = 10, 0.1  # Настройки попыток замены файла
    for _ in range(attempts):  # Цикл попыток замены файла
        try:
            os.replace(tmp, path)  # Атомарная замена файла
            return  # Выход при успехе
        except PermissionError:
            time.sleep(delay)
            delay = min(delay * 1.7, 1.0)
        except Exception:
            time.sleep(delay)
            delay = min(delay * 1.7, 1.0)
    try:
        path.write_text(payload, encoding="utf-8")
    except Exception as e:
        print(f"Критическая ошибка записи прогресса: {e}")


def load_progress(path: Path) -> dict[str, str]:
    '''
    Загружает прогресс парсинга из JSON файла.
    Return: Словарь с прогрессом или пустой словарь при ошибке
    '''
    if path.exists():  # Проверка существования файла
        try:
            return json.loads(path.read_text(encoding="utf-8"))  # Загрузка JSON данных
        except Exception as e:
            print(f"Не удалось прочитать существующий прогресс: {e}")
    return {}


def load_pending(path: Path) -> list[str]:
    '''
    Загружает список отложенных ссылок из JSON файла.
    Return: Список URL или пустой список при ошибке
    '''
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return [u for u in data if isinstance(u, str)]
        except Exception:
            pass
    return []


def save_pending(path: Path, urls: list[str]):
    '''
    Сохраняет список отложенных ссылок в JSON файл.
    '''
    urls = list(dict.fromkeys(urls))  # Уникальные, порядок сохраняем
    atomic_write_json(path, urls)


def dump_debug(page: Page, url: str):
    '''
    Сохраняет скриншот и HTML проблемной страницы для отладки.
    '''
    try:
        ad_id = get_avito_id_from_url(url)     # Получение ID объявления из URL
        png_path = DEBUG_DIR / f"{ad_id}.png"  # Пути
        html_path = DEBUG_DIR / f"{ad_id}.html"
        page.screenshot(path=str(png_path), full_page=True)  # Создание скриншота всей страницы
        html = safe_get_content(page)  # Получение HTML содержимого
        html_path.write_text(html, encoding="utf-8")
        print(f"🪪 Debug сохранён: {png_path.name}, {html_path.name}")
    except Exception as e:
        print(f"Не удалось сохранить debug: {e}")


# ЛОГИКА КЛИКА / ИЗВЛЕЧЕНИЯ

def click_show_phone_on_ad(page: Page) -> bool:
    '''
    Пытается найти и кликнуть на кнопку "Показать телефон" в объявлении.
    Return: True если кнопка найдена и клик выполнен
    '''
    human_scroll_jitter(page)

    for anchor in [
        "[data-marker='seller-info']",
        "[data-marker='item-sidebar']",
        "section:has(button[data-marker*='phone'])",
        "section:has(button:has-text('Показать'))",
    ]:
        try:
            a = page.query_selector(anchor)  # Поиск якорного элемента
            if a:
                a.scroll_into_view_if_needed()  # Прокрутка к элементу, если элемент найден
                human_sleep(*HUMAN["scroll_pause_s"])
                break
        except Exception:
            pass

    selector_groups = [
        [  # data-marker селекторы
            "button[data-marker='item-phone-button']",
            "button[data-marker='phone-button/number']",
            "button[data-marker*='phone-button']",
        ],
        [  # Текстовые селекторы
            "button:has-text('Показать телефон')",
            "button:has-text('Показать номер')",
        ],
    ]

    if HUMAN["randomize_selectors"]:
        random.shuffle(selector_groups)  # Перемешивание групп
        for g in selector_groups:
            random.shuffle(g)  # Перемешивание селекторов внутри группы

    try:
        page.wait_for_selector("button", timeout=2000)
    except Exception:
        pass

    for group in selector_groups:
        for sel in group:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible() and el.is_enabled():
                    if try_click(page, el):
                        print("Нажали 'Показать телефон'.")
                        
                        # Ждем появление номера или модалки авторизации
                        try:
                            # Ждем либо номер телефона, либо модалку авторизации
                            page.wait_for_selector(
                                "img[data-marker='phone-image'], [data-marker='login-form']", 
                                timeout=5000
                            )
                        except Exception:
                            pass
                        
                        # Проверяем, появилась ли модалка авторизации
                        if page.query_selector("[data-marker='login-form']"):
                            print("Обнаружена модалка авторизации после клика")
                            return False
                        
                        return True
            except Exception:
                continue

    print("Кнопка 'Показать телефон' не найдена.")
    return False

def extract_phone_data_uri_on_ad(page: Page) -> str | None:
    '''
    Извлекает data:image URI с изображением телефона со страницы. 
    Return: data:image URI или None если изображение не найдено
    '''
    try:  # Попытка поиска изображения телефона
        img = page.query_selector("img[data-marker='phone-image']")  # Поиск изображения по data-maker
    except PWError:
        img = None

    if not img or not img.is_visible():
        print("Картинка с номером не найдена.")
        return None

    # Получаем src атрибут
    try:
        src = img.get_attribute("src") or ""
    except Exception:
        img = None
    if not src.startswith("data:image"):
        print(f"src не data:image, а: {src[:60]}...")
        return None
    return src


# ПУЛ ВКЛАДОК (ТАБОВ) И ОБРАБОТКА СПИСКОВ

def make_page_pool(context, size: int) -> list[Page]:
    '''
    Создает пул страниц браузера.
    Return: Список объектов Page
    '''
    return [context.new_page() for _ in range(size)]  # Создание списка страниц


def process_urls_with_pool(context, urls: list[str], on_result, pending_queue: list[str]):
    '''
    Обрабатывает список URL с использованием пула страниц.
    Args:
        context: Контекст браузера Playwright
        urls: Список URL для обработки
        on_result: Функция обратного вызова для сохранения результатов
        pending_queue: Список для добавления отложенных URL
    '''
    if not urls:
        return

    # Пул создаём максимального размера; часть вкладок можем не использовать
    pages = make_page_pool(context, CONCURRENCY)
    try:
        it = iter(urls)  # Итератор по URL
        while True:
            # Иногда делаем партию меньше максимума, чтобы поведение было менее ровным
            batch_size = (
                random.randint(max(1, CONCURRENCY - 1), CONCURRENCY)
                if BATCH_CONCURRENCY_JITTER
                else CONCURRENCY
            )
            batch_pages = pages[:batch_size]

            batch = []  # Инициализация списка для текущей партии
            for idx, p in enumerate(batch_pages):  # Цикл по страницам партии
                try:
                    url = next(it)
                except StopIteration:
                    return
                batch.append((url, p))

                # Не открываем все вкладки синхронно — ставим паузу перед каждым goto
                human_sleep(*NAV_STAGGER_BETWEEN_TABS)
                try:
                    p.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                except PWTimeoutError:
                    print(f"Таймаут: {url}")
                    continue

                # Лёгкая «заминка» после навигации + пара скроллов
                human_sleep(*POST_NAV_IDLE)
                human_scroll_jitter(p, count=random.randint(1, 2))

            # Статус + модалки + попытка клика (тоже чуть «размазываем»)
            for url, p in batch:
                human_pause_jitter()
                st = classify_ad_status(p)
                if st == "blocked":
                    print(f"Капча/блок: {url}")
                    continue
                if st == "limit":
                    print(f"Лимит контактов: {url}")
                    on_result(url, TAG_LIMIT)
                    pending_queue.append(url)
                    continue
                if st == "unavailable":
                    print(f"Недоступно/закрыто: {url}")
                    on_result(url, TAG_UNAVAILABLE)
                    continue
                if st == "no_calls":
                    print(f"Без звонков: {url}")
                    on_result(url, TAG_NO_CALLS)
                    continue
                if st == "on_review":
                    print(f"На проверке: {url}")
                    on_result(url, TAG_ON_REVIEW)
                    pending_queue.append(url)
                    continue
                close_city_or_cookie_modals(p)
                if not click_show_phone_on_ad(p):
                    # Проверим ещё раз — вдруг это всё же on_review/limit/и т.д.
                    st2 = classify_ad_status(p)
                    if st2 == "limit":
                        on_result(url, TAG_LIMIT)
                        pending_queue.append(url)
                    elif st2 == "unavailable":
                        on_result(url, TAG_UNAVAILABLE)
                    elif st2 == "no_calls":
                        on_result(url, TAG_NO_CALLS)
                    if st2 == "on_review":
                        on_result(url, TAG_ON_REVIEW)
                        pending_queue.append(url)
                    else:
                        dump_debug(p, url)
            # Ждём картинку телефона (с небольшим джиттером между объявлениями)
            human_sleep(*HUMAN["click_delay_jitter"])
            for url, p in batch:
                human_pause_jitter()
                if close_login_modal_if_exists(p) or is_captcha_or_block(p):  # Проверка модалок и блокировок
                    continue  # Пропуск объявления 
                data_uri = extract_phone_data_uri_on_ad(p)
                if not data_uri:
                    continue
                if SAVE_DATA_URI:
                    value = data_uri
                else:
                    avito_id = get_avito_id_from_url(url)
                    out_path = save_phone_png_from_data_uri(data_uri, avito_id)
                    if not out_path:  # Проверка успешности сохранения
                        continue
                    value = out_path   # Использование пути к файлу
                on_result(url, value)  # Сохранение результата
                print(f"{url} -> {'[data:image...]' if SAVE_DATA_URI else value}")

            human_sleep(*PAGE_DELAY_BETWEEN_BATCHES)  # Пауза между партиями
    finally:
        for p in pages:
            try:
                human_sleep(*CLOSE_STAGGER_BETWEEN_TABS)
                p.close()  # Закрытие страницы
            except Exception:
                pass


def recheck_pending_once(context, on_result):
    '''
    Повторно проверяет отложенные ссылки.
    Args:
        context: Контекст браузера Playwright
        on_result: Функция обратного вызова для сохранения результатов
    '''
    pend = load_pending(PENDING_JSON)  # Загрузка отложенных ссылок
    if not pend:
        return
    print(f"\nПовторная проверка отложенных ссылок: {len(pend)}")
    page = context.new_page()  # Создание новой страницы для проверки
    still = []  # Список ссылок, которые остаются отложенными
    for url in pend:
        try:
            human_sleep(*NAV_STAGGER_BETWEEN_TABS)  # Пауза перед навигацией
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)  # Переход по URL
        except Exception:
            still.append(url)
            continue
        st = classify_ad_status(page)
        if st in ("on_review", "limit"):  # Проверка статусов, требующих повторной проверки
            still.append(url)
        elif st == "no_calls":
            on_result(url, TAG_NO_CALLS)
        elif st == "unavailable" or st == "blocked":
            on_result(url, TAG_UNAVAILABLE)
        else:
            # ok: пробуем кликнуть / считать
            close_city_or_cookie_modals(page)
            if click_show_phone_on_ad(page):
                time.sleep(random.uniform(*HUMAN["click_delay_jitter"]))
                data_uri = extract_phone_data_uri_on_ad(page)
                if data_uri:
                    if SAVE_DATA_URI:  # Режим сохранения data:image
                        on_result(url, data_uri)
                    else:
                        out = save_phone_png_from_data_uri(data_uri, get_avito_id_from_url(url))  # Сохранение PNG
                        if out:
                            on_result(url, out)  # Сохранение пути к файлу
                    print(f"(повтор) {url}")  # Логирование успеха
                else:
                    still.append(url)
            else: # Если сейчас стало «без звонков/недоступно»
                st2 = classify_ad_status(page)
                if st2 == "no_calls":
                    on_result(url, TAG_NO_CALLS)  # Сохранение результата
                elif st2 in ("on_review", "limit"):
                    still.append(url)
                else:
                    on_result(url, TAG_UNAVAILABLE)  # Сохранение как недоступного
        human_sleep(0.8, 1.6)
    try:
        page.close()
    except Exception:
        pass
    save_pending(PENDING_JSON, still)
    print(f"Осталось отложенных: {len(still)}")


# ОСНОВНОЙ СЦЕНАРИЙ

def main():
    '''
    Основная функция парсера.
    Координирует весь процесс парсинга телефонов с Avito.
    '''
    urls = read_urls_from_excel_or_csv(INPUT_FILE, INPUT_SHEET, URL_COLUMN)
    urls = urls[:TEST_TOTAL]

    phones_map: dict[str, str] = load_progress(OUT_JSON)
    already_done = set(phones_map.keys())
    urls = [u for u in urls if u not in already_done]

    # При старте — сначала очередь pending
    pending_queue = load_pending(PENDING_JSON)

    print(f"Новых ссылок к обработке: {len(urls)}; отложенных: {len(pending_queue)}")
    if not urls and not pending_queue:
        print(f"Нечего делать. Прогресс в {OUT_JSON}: {len(phones_map)} записей.")
        return

    def flush_progress():
        '''
        Внутренняя функция для сохранения прогресса.
        Вызывается при завершении программы.
        '''
        try:
            atomic_write_json(OUT_JSON, phones_map)    # Сохранение основного прогресса
            save_pending(PENDING_JSON, pending_queue)  # Сохранение отложенных ссылок
        except Exception as e:
            print(f"Ошибка записи прогресса: {e}")

    atexit.register(flush_progress)  # Регистрация функции при завершении программы
    for sig in ("SIGINT", "SIGTERM"):
        try:
            signal.signal(getattr(signal, sig), lambda *a: (flush_progress(), exit(1))) # Установка обработчика сигнала
        except Exception:
            pass

    with sync_playwright() as p:  # Создание контекста Playwright
        launch_kwargs = {         # Параметры запуска браузера
            "headless": HEADLESS, # Режим отображения браузера
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",  # max размер
            ],
        }
        if USE_PROXY:
            launch_kwargs["proxy"] = {
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_LOGIN,
                "password": PROXY_PASSWORD,
            }

        browser = p.chromium.launch(**launch_kwargs)  # Запуск браузера Chromium

        vp_w = random.randint(1200, 1400)
        vp_h = random.randint(760, 900)

        context = browser.new_context(  # Создание нового контекста браузера
            viewport={"width": vp_w, "height": vp_h},
            user_agent=UA,  # Установка User-Agent
        )
        context.set_default_navigation_timeout(NAV_TIMEOUT)  # Установка таймаута навигации
        context.set_default_timeout(NAV_TIMEOUT)

        # Ручной логин на первой ссылке (если есть что открывать)
        seed_url = pending_queue[0] if pending_queue else (urls[0] if urls else None)
        if seed_url:
            page = context.new_page() # Создание новой страницы
            try:
                page.goto(seed_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            except PWTimeoutError:
                pass
            print("\nТвои действия:")  # Инструкция пользователю
            print(" • если есть капча — реши;")
            print(" • залогинься в Авито;")
            print(" • оставь открытую страницу объявления.")
            input("Готов? Нажми Enter в консоли.\n")
            if is_captcha_or_block(page):
                print("Всё ещё капча/блок — выходим.")
                browser.close()
                flush_progress()
                return
            try:
                page.close()
            except Exception:
                pass

        def on_result(url: str, value: str | None):
            '''
            Функция обратного вызова для сохранения результатов.
            Args:
                url: URL объявления
                value: data:image..., путь к PNG или __SKIP_*__
            '''
            if value is None:
                return
            phones_map[url] = value
            atomic_write_json(OUT_JSON, phones_map) # Сохранение прогресса

        # Обработка отложенных ссылок (сняв уже обработанные)
        pending_queue = [u for u in pending_queue if u not in already_done]
        try:
            process_urls_with_pool(
                context, pending_queue, on_result, pending_queue
            )  # Обработка с добавлением новых отложенных в конец
        except KeyboardInterrupt:
            print("Остановлено пользователем (на pending).")
            flush_progress()  # Сохранение прогресса

        # Перепроверка оставшихся отложенных
        recheck_pending_once(context, on_result)

        # Основной список из Excel
        try:
            process_urls_with_pool(context, urls, on_result, pending_queue)
        except KeyboardInterrupt:
            print("Остановлено пользователем (на основных ссылках).")
            flush_progress()

        browser.close()
        flush_progress()
        print(
            f"\nГотово. В {OUT_JSON} сейчас {len(phones_map)} записей. "
            f"Отложенных осталось: {len(load_pending(PENDING_JSON))}"
        )


if __name__ == "__main__":
    main()
