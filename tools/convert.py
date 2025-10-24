import json

# путь к исходному файлу
input_path = "news.jsonl"     # исходный файл (каждая строка — JSON)
output_path = "news.json"     # куда сохранить объединённый JSON

data = []
with open(input_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            data.append(obj)
        except json.JSONDecodeError as e:
            print(f"⚠️ Пропущена строка: {e}")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"✅ Сохранено {len(data)} записей в файл: {output_path}")
