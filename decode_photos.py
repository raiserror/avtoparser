import json
import base64
import re
from io import BytesIO
from pathlib import Path

from PIL import Image
import pytesseract

# Если нужно, укажите путь к tesseract, например:
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
INPUT_JSON = Path("avito_phones_playwright/phones_map.json")
OUTPUT_JSON = Path("phones_out.json")


def to_avito_url(key: str) -> str:
    """
    Преобразует ключи вида:
      - "/moskva/kvartiry/....?context=..." -> "https://www.avito.ru/moskva/kvartiry/..."
      - "https://www.avito.ru/....?context=..." -> "https://www.avito.ru/..."
      - оставляет прочие http-ссылки без изменений, но обрезает query.
    """
    if key.startswith("http://") or key.startswith("https://"):
        base = key
    elif key.startswith("/"):
        base = "https://www.avito.ru" + key
    else:
        # Если вдруг пришло что-то иное — просто вернём как есть
        base = key
    # Убираем query-параметры типа ?context=...
    base = base.split("?", 1)[0]
    return base


def decode_img_phones(data: dict) -> dict:
    final_data = {}
    phone_pattern = re.compile(
        r'(?:\+7|7|8)?[\s\-()]*(\d{3})[\s\-()]*(\d{3})[\s\-()]*(\d{2})[\s\-()]*(\d{2})'
    )

    def normalize_phone(match):
        g = match.groups()
        return "+7" + "".join(g)

    for raw_url, data_url in data.items():
        # Нормализуем ссылку в формат https://www.avito.ru/... (без ?context=...)
        url = to_avito_url(raw_url)

        # Извлекаем base64
        if "," in data_url:
            _, b64_data = data_url.split(",", 1)
        else:
            b64_data = data_url

        # Декодируем в картинку
        img_bytes = base64.b64decode(b64_data)
        img = Image.open(BytesIO(img_bytes))
        # При желании можно чуть помочь OCR:
        # img = img.convert("L")

        # OCR (рус+англ, чтобы видеть +7 / текст)
        text = pytesseract.image_to_string(img, lang="rus+eng")

        # Ищем телефоны
        phones = {normalize_phone(m) for m in phone_pattern.finditer(text)}

        if phones:
            # Если несколько — берём любой (множество), при желании можно объединять/сортировать
            final_data[url] = next(iter(phones))
        else:
            print(f"[no phone] {url}")
    return final_data


if __name__ == "__main__":
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Файл не найден: {INPUT_JSON.resolve()}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        src = json.load(f)
        if not isinstance(src, dict):
            raise ValueError("Ожидался JSON-объект {url: data_uri}")

    result = decode_img_phones(src)

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"OK: найдено {len(result)} телефонов -> {OUTPUT_JSON.resolve()}")
