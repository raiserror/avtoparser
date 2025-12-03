import json
import sys
from pathlib import Path

import pandas as pd
from decode_photos import decode_img_phones

INPUT_JSON = Path("avito_phones_playwright/phones_map.json")
OUTPUT_XLSX = Path("phones.xlsx")

def load_data(path: Path) -> dict:
    if not path.exists():
        print(f"Файл не найден: {path.resolve()}")
        sys.exit(1)
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            print("Ожидался JSON-объект {url: data_uri}, а не массив/другое.")
            sys.exit(2)
        return data
    except json.JSONDecodeError as e:
        print(f"Некорректный JSON в {path}: {e}")
        sys.exit(3)

def save_to_excel(url2phone: dict, out_path: Path):
    if not url2phone:
        print("Телефоны не найдены. Excel не создан.")
        return
    rows = [{"url": u, "phone": p} for u, p in url2phone.items()]
    df = pd.DataFrame(rows, columns=["url", "phone"])
    df.to_excel(out_path, index=False)
    print(f"OK: сохранено {len(df)} строк -> {out_path.resolve()}")

if __name__ == "__main__":
    data = load_data(INPUT_JSON)

    # Фильтрация служебных пометок
    filtered = {
        url: val
        for url, val in data.items()
        if isinstance(val, str) and not val.startswith("__SKIP")
    }
    print(f"Всего записей: {len(data)} | после фильтрации: {len(filtered)}")

    result = decode_img_phones(filtered)
    if not result:
        print("decode_img_phones не извлек ни одного телефона.")
    save_to_excel(result, OUTPUT_XLSX)