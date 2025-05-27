import json
from pathlib import Path

def flatten_with_array_levels(obj, path="", level=0, results=None):
    if results is None:
        results = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            flatten_with_array_levels(v, new_path, level, results)
    elif isinstance(obj, list):
        results.append((path, level))
        for item in obj:
            flatten_with_array_levels(item, path, level + 1, results)
    else:
        results.append((path, level))

    return results

if __name__ == "__main__":
    # Load JSON from file
    json_file = Path("sample.json")
    with open(json_file, "r") as f:
        data = json.load(f)

    flattened = flatten_with_array_levels(data)
    
    # Deduplicate
    seen = set()
    output_lines = ["element,level"]
    for path, lvl in flattened:
        if path not in seen:
            seen.add(path)
            output_lines.append(f"{path},{lvl}")

    # Save to output.txt
    with open("output.txt", "w") as f_out:
        f_out.write("\n".join(output_lines))

    print("Flattened output written to output.txt")
