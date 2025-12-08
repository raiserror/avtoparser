# avito_async.py
import json, random, asyncio, time
from pathlib import Path
from typing import Optional, Dict, List

from playwright.async_api import (
    async_playwright,
    Page,
    TimeoutError as PWTimeoutError,
    Error as PWError,
)

# НАСТРОЙКИ
CATEGORY_URL = "https://www.avito.ru/moskva/kvartiry/sdam/na_dlitelnyy_srok-ASgBAgICAkSSA8gQ8AeQUg?user=1"
OUT_DIR = Path("avito_phones_playwright")
OUT_DIR.mkdir(exist_ok=True)

HEADLESS = False
MAX_ITEMS = 100  # ОБЪЯВЛЕНИЙ С НАЙДЕННОЙ КАРТИНКОЙ НОМЕРА
MAX_CONCURRENT_TASKS = 3
PAGE_DELAY = 3
CLICK_DELAY = 5
NAV_TIMEOUT = 60_000

USE_PROXY = False
PROXY_HOST = "mproxy.site"
PROXY_PORT = 228
PROXY_LOGIN = ""
PROXY_PASSWORD = ""

# ХЕЛПЕРЫ

async def human_sleep(a: float, b: float):
    """Случайная задержка"""
    await asyncio.sleep(random.uniform(a, b))

async def safe_get_content(page: Page) -> str:
    """Безопасное получение содержимого"""
    for _ in range(2):
        try:
            return await page.content()
        except PWError:
            await asyncio.sleep(0.7)
    return ""

async def is_captcha_or_block(page: Page) -> bool:
    """Быстрая проверка на блокировку"""
    try:
        url = (page.url or "").lower()
    except PWError:
        url = ""
    html = (await safe_get_content(page)).lower()
    return (
        "captcha" in url or 
        "firewall" in url or
        "доступ с вашего ip-адреса временно ограничен" in html
    )

async def close_city_or_cookie_modals(page: Page):
    """Быстрое закрытие модальных окон"""
    selectors = [
        "button[aria-label='Закрыть']",
        "button[data-marker='modal-close']",
        "button[class*='close']",
        "button:has-text('Понятно')",
        "button:has-text('Хорошо')",
    ]
    for sel in selectors:
        try:
            for b in await page.query_selector_all(sel):
                try:
                    if await b.is_visible():
                        await b.click()
                        await human_sleep(0.2, 0.5)
                except Exception:
                    continue
        except Exception:
            continue

async def close_login_modal_if_exists(page: Page) -> bool:
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
            modals = await page.query_selector_all(sel)
        except PWError:
            continue

        for m in modals:
            if not await m.is_visible():
                continue

            for btn_sel in close_selectors:
                btn = await m.query_selector(btn_sel)
                if btn:
                    try:
                        if await btn.is_enabled():
                            await btn.click()
                            await human_sleep(0.4, 0.8)
                            print("Модалка авторизации закрыта, объявление пропущено.")
                            return True
                    except Exception:
                        pass

            print("Модалка авторизации не закрывается — объявление пропускаем.")
            return True

    return False

async def extract_phone_image_data(page: Page, avito_id: str) -> Optional[str]:
    """
    После клика ищем img[data-marker='phone-image'],
    возвращаем data:image/png;base64,... (без сохранения PNG).
    """
    try:
        img = await page.query_selector("img[data-marker='phone-image']")
        if img:
            src = await img.get_attribute("src")
            if src and src.startswith("data:image"):
                return src
    except Exception as e:
        print(f"Ошибка при извлечении номера: {e}")

    # Альтернативно: ищем текст с номером
    try:
        phone_text = await page.query_selector("[data-marker='phone-popup']")
        if phone_text:
            text_content = await phone_text.text_content()
            if text_content and any(c.isdigit() for c in text_content):
                digits = ''.join(filter(str.isdigit, text_content))
                if 9 <= len(digits) <= 12:
                    return digits
    except:
        pass
    
    return None

async def process_single_item(item, page: Page, idx: int, semaphore: asyncio.Semaphore) -> Optional[tuple[str, str]]:
    """Обработка одной карточки с семафором"""
    async with semaphore:
        try:
            # Получаем URL объявления
            url_el = await item.query_selector('a[itemprop="url"]')
            if not url_el:
                return None
            
            url = await url_el.get_attribute("href")
            if not url:
                return None
            
            avito_id = (await item.get_attribute("id") or "").lstrip("i")
            print(f"[{avito_id}] Обработка карточки #{idx}...")
            
            # hover
            try:
                await item.hover()
                await human_sleep(0.3, 0.7)
            except Exception:
                pass

            # Ищем кнопку телефона
            btn_selectors = [
                "button[data-marker='item-phone-button']",
                "button:has-text('Показать телефон')",
                "button:has-text('Показать номер')",
                "button[aria-label*='Показать телефон']",
                "button[aria-label*='Показать номер']",
            ]
            
            phone_button = None
            for sel in btn_selectors:
                try:
                    b = await item.query_selector(sel)
                    if b and await b.is_enabled() and await b.is_visible():
                        phone_button = b
                        break
                except Exception:
                    continue

            if not phone_button:
                print(f"[{avito_id}] Кнопка 'Показать телефон' не найдена.")
                return None

            await human_sleep(0.5, 1.5)

            # Кликаем
            try:
                await phone_button.scroll_into_view_if_needed()
                await human_sleep(0.2, 0.5)
                await phone_button.click()
                print(f"[{avito_id}] Нажали 'Показать телефон' (#{idx}).")
            except Exception as e:
                print(f"[{avito_id}] Не удалось кликнуть: {e}")
                return None

            # Ожидание после клика
            await asyncio.sleep(CLICK_DELAY)

            # Проверка модалок и капчи
            if await close_login_modal_if_exists(page):
                return None
            
            if await is_captcha_or_block(page):
                print("Капча/блок после клика телефона.")
                return None

            # Извлекаем номер
            phone_data = await extract_phone_image_data(page, avito_id)
            
            if phone_data:
                print(f"[{avito_id}] Успешно получен номер")
                return (url, phone_data)
            
            return None
            
        except Exception as e:
            print(f"Ошибка при обработке карточки #{idx}: {e}")
            return None

async def process_items_concurrently(page: Page, items: List) -> Dict[str, str]:
    """Параллельная обработка карточек"""
    phones_map = {}
    
    # Создаем семафор для ограничения одновременных операций
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    # Создаем задачи для обработки
    tasks = []
    for idx, item in enumerate(items[:MAX_ITEMS], 1):
        task = process_single_item(item, page, idx, semaphore)
        tasks.append(task)
    
    # Запускаем все задачи параллельно
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем результаты
    for result in results:
        if isinstance(result, Exception):
            print(f"Исключение в задаче: {result}")
            continue
            
        if result:
            url, phone_data = result
            phones_map[url] = phone_data
    
    return phones_map

async def main():
    start_time = time.time()
    
    # Конфигурация браузера
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        context.set_default_navigation_timeout(NAV_TIMEOUT)
        context.set_default_timeout(NAV_TIMEOUT)

        page = await context.new_page()

        # Переход на страницу категории
        print(f"Открываем {CATEGORY_URL}")
        try:
            await page.goto(CATEGORY_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except PWTimeoutError:
            print("Таймаут навигации — продолжаем с тем, что есть...")

        # РУЧНОЙ ВХОД
        print("\nВаши действия:")
        print(" • Если есть капча — решите;")
        print(" • Войдите в аккаунт;")
        print(" • Вернитесь на страницу с объявлениями.")
        input("Когда на экране список объявлений, нажми Enter в консоли.\n")

        await asyncio.sleep(2)

        # Проверка блокировок
        if await is_captcha_or_block(page):
            print("Обнаружена блокировка - выход.")
            await browser.close()
            return

        await close_city_or_cookie_modals(page)

        # Ожидание загрузки объявлений
        try:
            await page.wait_for_selector('div[data-marker="item"]', timeout=30000)
        except PWTimeoutError:
            print("Не видим объявлений.")
            await browser.close()
            return

        print(f"Ждём {PAGE_DELAY} секунд перед обработкой...")
        await asyncio.sleep(PAGE_DELAY)

        # Получаем все карточки
        items = await page.query_selector_all('div[data-marker="item"]')
        print(f"Найдено карточек: {len(items)}")
        
        if not items:
            print("Нет карточек для обработки")
            await browser.close()
            return
        
        # Параллельная обработка карточек
        phones_map = await process_items_concurrently(page, items)
        
        elapsed_time = time.time() - start_time
        
        # Сохранение результатов
        if phones_map:
            out_file = OUT_DIR / "phones" / "phones_fast.json"
            out_file.write_text(
                json.dumps(phones_map, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            
            print(f"\n=== РЕЗУЛЬТАТ ===")
            print(f"Успешно получено номеров: {len(phones_map)}")
            print(f"Время выполнения: {elapsed_time:.1f} секунд")
            print(f"Файл сохранён: {out_file}")
        else:
            print("Нет данных для сохранения")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())