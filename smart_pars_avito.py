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

# НАСТРОЙКИ


# ВХОДНОЙ ФАЙЛ С ССЫЛКАМИ
INPUT_FILE = Path("РЕМОНТ МСК МО 13.11.xlsx")  # Имя Excel/CSV-файла с ссылками на объявления

INPUT_SHEET = None  # Имя листа в Excel; None = использовать все листы
URL_COLUMN = None   # Имя колонки со ссылками; None = искать ссылки во всех колонках

# ПАПКИ И ОСНОВНЫЕ ВЫХОДНЫЕ ФАЙЛЫ
OUT_DIR = Path("avito_phones_playwright")  # Рабочая директория парсера
OUT_DIR.mkdir(exist_ok=True)
IMG_DIR = (OUT_DIR / "phones")  # Сюда будут сохраняться PNG с номерами (если SAVE_DATA_URI = False  (То что не провряли давно и не используется))
IMG_DIR.mkdir(exist_ok=True)
DEBUG_DIR = OUT_DIR / "debug"  # Сюда складываем скриншоты и html проблемных объявлений
DEBUG_DIR.mkdir(exist_ok=True)

OUT_JSON = (OUT_DIR / "phones_map.json")          # Основной результат: {url: data:image... или тег __SKIP_*__}
PENDING_JSON = (OUT_DIR / "pending_review.json")  # Ссылки «на модерации» и с лимитом контактов (в разработке на будущее)
SAVE_DATA_URI = (True)                            # True = сохраняем data:image в JSON; False = сохраняем PNG в IMG_DIR
HEADLESS = False                                  # False = браузер виден (можно логиниться руками)

# ОБЪЁМ И ПАРАЛЛЕЛЬНОСТЬ
TEST_TOTAL = 766  # Максимум объявлений за один запуск (обрежется по списку ссылок)
CONCURRENCY = 3   # Сколько вкладок (tab-ов) одновременно открыто (2–3 оптимально)


# БАЗОВЫЕ ТАЙМАУТЫ
CLICK_DELAY = 8       # Базовая задержка перед ожиданием появления картинки с номером
NAV_TIMEOUT = 90_000  # Таймаут загрузки страницы, мс (90 секунд)


# НАСТРОЙКИ ПРОКСИ
USE_PROXY = False                # True = использовать прокси, False = напрямую
PROXY_HOST = "mproxy.site"       # Адрес прокси-сервера
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
    "mouse_wiggle_steps": (2, 5),             # Сколько шагов этих «подёргиваний»
    "between_actions_pause": (0.10, 0.30, ),  # Пауза между действиями (скролл, клик, наведение)
    "click_delay_jitter": (
        CLICK_DELAY * 0.9,
        CLICK_DELAY * 1.25,
    ),  # Разброс ожидания после клика по телефону
    "randomize_selectors": True,  # Иногда менять порядок селекторов, чтобы не бить всегда в один и тот же
}


# Теги в phones_map.json при пропусках
TAG_NO_CALLS = "__SKIP_NO_CALLS__"        # Объявление «без звонков» / только сообщения
TAG_UNAVAILABLE = "__SKIP_UNAVAILABLE__"  # Объявление закрыто/удалено/недоступно
TAG_ON_REVIEW = "__SKIP_ON_REVIEW__"      # Объявление ещё на модерации
TAG_LIMIT = "__SKIP_LIMIT__"              # Закончился лимит показа контактов на аккаунте


# ХЕЛПЕРЫ

def human_sleep(a: float, b: float):
    time.sleep(random.uniform(a, b))


def human_pause_jitter():
    human_sleep(*HUMAN["between_actions_pause"])


def human_scroll_jitter(page: Page, count: int | None = None):
    if count is None:
        count = random.randint(*HUMAN["pre_page_warmup_scrolls"])
    try:
        height = page.evaluate("() => document.body.scrollHeight") or 3000
        for _ in range(count):
            step = random.randint(*HUMAN["scroll_step_px"])
            direction = 1 if random.random() > 0.25 else -1
            y = max(0, min(height, page.evaluate("() => window.scrollY") + step * direction))
            page.evaluate("y => window.scrollTo({top: y, behavior: 'smooth'})", y)
            human_sleep(*HUMAN["scroll_pause_s"])
    except Exception:
        pass


def human_wiggle_mouse(page: Page, x: float, y: float):
    steps = random.randint(*HUMAN["mouse_wiggle_steps"])
    amp = random.randint(*HUMAN["mouse_wiggle_px"])
    for _ in range(steps):
        dx = random.randint(-amp, amp)
        dy = random.randint(-amp, amp)
        try:
            page.mouse.move(x + dx, y + dy)
        except Exception:
            pass
        human_pause_jitter()


def human_hover(page: Page, el):
    try:
        box = el.bounding_box()
        if not box:
            return
        cx = box["x"] + box["width"] * random.uniform(0.35, 0.65)
        cy = box["y"] + box["height"] * random.uniform(0.35, 0.65)
        page.mouse.move(cx, cy)
        human_wiggle_mouse(page, cx, cy)
        human_sleep(*HUMAN["hover_pause_s"])
    except Exception:
        pass


def safe_get_content(page: Page) -> str:
    for _ in range(2):
        try:
            return page.content()
        except PWError:
            time.sleep(1)
    return ""



def is_captcha_or_block(page: Page) -> bool:
    try:
        url = page.url.lower()
    except PWError:
        url = ""
    html = safe_get_content(page).lower()
    return (
        "captcha" in url or 
        "firewall" in url or
        "доступ с вашего ip-адреса временно ограничен" in html
    )


def close_city_or_cookie_modals(page: Page):
    selectors = [
        "button[aria-label='Закрыть']",
        "button[data-marker='modal-close']",
        "button[class*='close']",
        "button:has-text('Понятно')",
        "button:has-text('Хорошо')",
        "button:has-text('Согласен')",
        "button:has-text('Принять')",
    ]
    for b in page.query_selector_all(selectors):
        try:
            if b.is_visible():
                human_hover(page, b)
                b.click()
                human_sleep(0.25, 0.7)
        except Exception:
            continue


def close_login_modal_if_exists(page: Page) -> bool:
    """Если вылезла авторизация после клика — закрываем и считаем объявление неудачным."""
    selectors_modal = [
        "[data-marker='login-form']",
        "[data-marker='registration-form']",
        "div[class*='modal'][class*='auth']",
        "div[class*='modal'] form[action*='login']",
    ]
    close_selectors = [
        "button[aria-label='Закрыть']",
        "button[data-marker='modal-close']",
        "button[class*='close']",
        "button[type='button']",
    ]
    for sel in selectors_modal:
        try:
            modals = page.query_selector_all(sel)
        except PWError:
            continue
        for m in modals:
            if not m.is_visible():
                continue
            for btn_sel in close_selectors:
                btn = m.query_selector(btn_sel)
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
    try:
        _, b64_data = data_uri.split(",", 1)
        raw = b64decode(b64_data)
        image = Image.open(BytesIO(raw)).convert("RGB")
        file_name = f"{file_stem}.png"
        out_path = IMG_DIR / file_name
        image.save(out_path)
        print(f"PNG сохранён: {out_path}")
        return str(out_path)
    except Exception as e:
        print(f"Ошибка при сохранении PNG: {e}")
        return None


def get_avito_id_from_url(url: str) -> str:
    m = re.search(r"(\d{7,})", url)
    return m.group(1) if m else str(int(time.time()))


def try_click(page: Page, el) -> bool:
    try:
        el.scroll_into_view_if_needed()
    except Exception:
        pass
    human_hover(page, el)
    human_sleep(*HUMAN["pre_click_pause_s"])
    try:
        el.click()
        human_sleep(*HUMAN["post_click_pause_s"])
        return True
    except Exception:
        try:
            box = el.bounding_box() or {}
            if box:
                page.mouse.move(box.get("x", 0) + 6, box.get("y", 0) + 6)
                human_sleep(*HUMAN["pre_click_pause_s"])
            page.evaluate("(e)=>e.click()", el)
            human_sleep(*HUMAN["post_click_pause_s"])
            return True
        except Exception:
            return False


# ПРОВЕРКА "ЛИМИТ КОНТАКТОВ"
def is_limit_contacts_modal(page: Page) -> bool:
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
    """
    'ok' | 'no_calls' | 'on_review' | 'unavailable' | 'blocked' | 'limit'
    """
    if is_captcha_or_block(page):
        return "blocked"

    html = safe_get_content(page).lower()

    if is_limit_contacts_modal(page):
        return "limit"
    if any(m in html for m in MODERATION_MARKERS):
        return "on_review"
    if any(m in html for m in UNAVAILABLE_MARKERS):
        return "unavailable"
    if any(m in html for m in NO_CALLS_MARKERS):
        return "no_calls"

    try:
        if page.locator("text=Без звонков").first.is_visible():
            return "no_calls"
    except Exception:
        pass

    return "ok"


# ВХОДНЫЕ URL ИЗ Excel/CSV

def read_urls_from_excel_or_csv(path: Path, sheet=None, url_column=None) -> list[str]:
    url_re = re.compile(r'https?://(?:www\.)?avito\.ru/[^\s"]+')
    urls: list[str] = []

    if path.suffix.lower() in {".xlsx", ".xls"}:
        xls = pd.ExcelFile(path)
        sheets = [sheet] if sheet is not None else xls.sheet_names
        for sh in sheets:
            df = xls.parse(sh, dtype=str)
            if url_column and url_column in df.columns:
                col = df[url_column].dropna().astype(str)
                urls.extend(col.tolist())
            else:
                for col in df.columns:
                    s = df[col].dropna().astype(str)
                    for val in s:
                        urls.extend(url_re.findall(val))
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
    seen = set()
    for u in urls:
        u = u.strip()
        if not u.startswith("http"):
            u = urljoin("https://www.avito.ru", u)
        u = u.split("#", 1)[0]
        u = u.split("?", 1)[0]
        if u not in seen:
            seen.add(u)
            cleaned.append(u)
    return cleaned


# БЕЗОПАСНОЕ СОХРАНЕНИЕ / ЧТЕНИЕ ПРОГРЕССА

def atomic_write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + f".tmp_{int(time.time()*1000)}_{random.randint(1000,9999)}")
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    attempts, delay = 10, 0.1
    for _ in range(attempts):
        try:
            os.replace(tmp, path)
            return
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
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Не удалось прочитать существующий прогресс: {e}")
    return {}


def load_pending(path: Path) -> list[str]:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return [u for u in data if isinstance(u, str)]
        except Exception:
            pass
    return []


def save_pending(path: Path, urls: list[str]):
    urls = list(dict.fromkeys(urls))  # Уникальные, порядок сохраняем
    atomic_write_json(path, urls)


def dump_debug(page: Page, url: str):
    try:
        ad_id = get_avito_id_from_url(url)
        png_path = DEBUG_DIR / f"{ad_id}.png"
        html_path = DEBUG_DIR / f"{ad_id}.html"
        page.screenshot(path=str(png_path), full_page=True)
        html = safe_get_content(page)
        html_path.write_text(html, encoding="utf-8")
        print(f"🪪 Debug сохранён: {png_path.name}, {html_path.name}")
    except Exception as e:
        print(f"Не удалось сохранить debug: {e}")


# ЛОГИКА КЛИКА / ИЗВЛЕЧЕНИЯ

def click_show_phone_on_ad(page: Page) -> bool:
    human_scroll_jitter(page)

    for anchor in [
        "[data-marker='seller-info']",
        "[data-marker='item-sidebar']",
        "section:has(button[data-marker*='phone'])",
        "section:has(button:has-text('Показать'))",
    ]:
        try:
            a = page.query_selector(anchor)
            if a:
                a.scroll_into_view_if_needed()
                human_sleep(*HUMAN["scroll_pause_s"])
                break
        except Exception:
            pass

    selector_groups = [
        [
            "button[data-marker='item-phone-button']",
            "button[data-marker='phone-button/number']",
            "button[data-marker*='phone-button']",
        ],
        [
            "button:has-text('Показать телефон')",
            "button:has-text('Показать номер')",
            "a:has-text('Показать телефон')",
            "a:has-text('Показать номер')",
        ],
        [
            "button[aria-label*='Показать телефон']",
            "button[aria-label*='Показать номер']",
        ],
        [
            "[data-marker*='phone'] button",
            "[data-marker*='contacts'] button",
        ],
    ]

    if HUMAN["randomize_selectors"]:
        random.shuffle(selector_groups)
        for g in selector_groups:
            random.shuffle(g)

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
                        return True
            except Exception:
                continue

    try:
        sticky = page.query_selector("footer:has(button)")
        if sticky:
            btn = sticky.query_selector("button")
            if btn and btn.is_visible() and btn.is_enabled():
                if try_click(page, btn):
                    print("Нажали кнопку в липком футере.")
                    return True
    except Exception:
        pass

    print("Кнопка 'Показать телефон' не найдена.")
    return False


def extract_phone_data_uri_on_ad(page: Page) -> str | None:
    try:
        img = page.query_selector("img[data-marker='phone-image']")
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
    return [context.new_page() for _ in range(size)]


def process_urls_with_pool(
    context, urls: list[str], on_result, pending_queue: list[str]
):
    """Основной проход: переиспользуем вкладки и ждём DOMContentLoaded; добавлены рассинхроны."""
    if not urls:
        return

    # Пул создаём максимального размера; часть вкладок можем не использовать
    pages = make_page_pool(context, CONCURRENCY)
    try:
        it = iter(urls)
        while True:
            # Иногда делаем партию меньше максимума, чтобы поведение было менее ровным
            batch_size = (
                random.randint(max(1, CONCURRENCY - 1), CONCURRENCY)
                if BATCH_CONCURRENCY_JITTER
                else CONCURRENCY
            )
            batch_pages = pages[:batch_size]

            batch = []
            for idx, p in enumerate(batch_pages):
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
                if st == "on_review":
                    print(f"На проверке: {url}")
                    on_result(url, TAG_ON_REVIEW)
                    pending_queue.append(url)
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

                close_city_or_cookie_modals(p)
                if not click_show_phone_on_ad(p):
                    # Проверим ещё раз — вдруг это всё же on_review/limit/и т.д.
                    st2 = classify_ad_status(p)
                    if st2 == "on_review":
                        on_result(url, TAG_ON_REVIEW)
                        pending_queue.append(url)
                    elif st2 == "limit":
                        on_result(url, TAG_LIMIT)
                        pending_queue.append(url)
                    elif st2 == "unavailable":
                        on_result(url, TAG_UNAVAILABLE)
                    elif st2 == "no_calls":
                        on_result(url, TAG_NO_CALLS)
                    else:
                        dump_debug(p, url)

            # Ждём картинку телефона (с небольшим джиттером между объявлениями)
            human_sleep(*HUMAN["click_delay_jitter"])
            for url, p in batch:
                human_pause_jitter()
                if close_login_modal_if_exists(p) or is_captcha_or_block(p):
                    continue
                data_uri = extract_phone_data_uri_on_ad(p)
                if not data_uri:
                    continue
                if SAVE_DATA_URI:
                    value = data_uri
                else:
                    avito_id = get_avito_id_from_url(url)
                    out_path = save_phone_png_from_data_uri(data_uri, avito_id)
                    if not out_path:
                        continue
                    value = out_path
                on_result(url, value)
                print(f"{url} -> {'[data:image...]' if SAVE_DATA_URI else value}")

            # Пауза между партиями — тоже чуть шире
            human_sleep(*PAGE_DELAY_BETWEEN_BATCHES)
    finally:
        for p in pages:
            try:
                human_sleep(*CLOSE_STAGGER_BETWEEN_TABS)
                p.close()
            except Exception:
                pass


# ПЕРЕПРОВЕРКА ОЧЕРЕДИ PENDING (КОРОТКИЙ ПРОХОД)

def recheck_pending_once(context, on_result):
    pend = load_pending(PENDING_JSON)
    if not pend:
        return
    print(f"\nПовторная проверка отложенных ссылок: {len(pend)}")
    page = context.new_page()
    still = []
    for url in pend:
        try:
            human_sleep(*NAV_STAGGER_BETWEEN_TABS)  # Тоже не открываем «в ноль»
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except Exception:
            still.append(url)
            continue
        st = classify_ad_status(page)
        if st in ("on_review", "limit"):
            still.append(url)  # Пока рано
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
                    if SAVE_DATA_URI:
                        on_result(url, data_uri)
                    else:
                        out = save_phone_png_from_data_uri(data_uri, get_avito_id_from_url(url))
                        if out:
                            on_result(url, out)
                    print(f"(повтор) {url}")
                else:
                    still.append(url)
            else:
                # Если сейчас стало «без звонков/недоступно»
                st2 = classify_ad_status(page)
                if st2 == "no_calls":
                    on_result(url, TAG_NO_CALLS)
                elif st2 in ("on_review", "limit"):
                    still.append(url)
                else:
                    on_result(url, TAG_UNAVAILABLE)
        human_sleep(0.8, 1.6)
    try:
        page.close()
    except Exception:
        pass
    save_pending(PENDING_JSON, still)
    print(f"Осталось отложенных: {len(still)}")


# ОСНОВНОЙ СЦЕНАРИЙ

def main():
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
        try:
            atomic_write_json(OUT_JSON, phones_map)
            save_pending(PENDING_JSON, pending_queue)
        except Exception as e:
            print(f"Ошибка записи прогресса: {e}")

    atexit.register(flush_progress)
    for sig in ("SIGINT", "SIGTERM"):
        try:
            signal.signal(getattr(signal, sig), lambda *a: (flush_progress(), exit(1)))
        except Exception:
            pass

    with sync_playwright() as p:
        launch_kwargs = {
            "headless": HEADLESS,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
        }
        if USE_PROXY:
            launch_kwargs["proxy"] = {
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_LOGIN,
                "password": PROXY_PASSWORD,
            }

        browser = p.chromium.launch(**launch_kwargs)

        vp_w = random.randint(1200, 1368)
        vp_h = random.randint(760, 900)

        context = browser.new_context(
            viewport={"width": vp_w, "height": vp_h},
            user_agent=UA,
        )
        context.set_default_navigation_timeout(NAV_TIMEOUT)
        context.set_default_timeout(NAV_TIMEOUT)

        # Ручной логин на первой ссылке (если есть что открывать)
        seed_url = pending_queue[0] if pending_queue else (urls[0] if urls else None)
        if seed_url:
            page = context.new_page()
            try:
                page.goto(seed_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            except PWTimeoutError:
                pass
            print("\nТвои действия:")
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
            # value: data:image..., путь к PNG или __SKIP_*__
            if value is None:
                return
            phones_map[url] = value
            atomic_write_json(OUT_JSON, phones_map)

        # 1) Сначала обрабатываем pending (сняв уже обработанные)
        pending_queue = [u for u in pending_queue if u not in already_done]
        try:
            process_urls_with_pool(
                context, pending_queue, on_result, pending_queue
            )  # Новые «pending» добавятся в конец
        except KeyboardInterrupt:
            print("Остановлено пользователем (на pending).")
            flush_progress()

        # 2) Короткая перепроверка того, что ещё осталось в pending после шага 1
        recheck_pending_once(context, on_result)

        # 3) Теперь основной список из Excel
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
