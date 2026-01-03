import subprocess
import time

pages = [
    ("3d8903c6-676c-4bcd-b3d2-8b4547275d06", "forklift rental"),
    ("80958e6b-937e-47bb-ab9c-726071548e00", "excavator rental"),
    ("6320582b-8c0d-4c17-a7a3-8b8c7d634220", "Excavator"),
    ("73e5a734-4fdc-435a-8b1c-748e0b1b9502", "excavator financing"),
    ("263481ad-76c4-4ecc-ba49-2f74b8abde18", "bulldozer rental"),
    ("7fd3ea11-b144-49e8-9c99-0b675756a053", "Bulldozer"),
    ("4799fbb3-4d7d-4d11-bc59-30a7289fc18e", "Forklift"),
    ("be2854da-e9c6-43a1-9730-920fe6877ebf", "bulldozer for sale"),
    ("ac4ef222-31ec-4092-a957-37403c4adc4e", "excavator for sale"),
    ("892c2d29-b15e-4bf0-a89b-c9f32ae382d2", "Crane"),
    ("87df08ba-bc8b-46be-b388-a933f172d638", "crane financing"),
    ("885adc07-665b-4227-98e8-44edba99206d", "crane for sale"),
    ("1d1da8d0-e25f-4a92-8946-f4025943de1e", "crane rental"),
    ("479615bc-f210-4559-9e03-4046097a06d5", "forklift for sale"),
]

print(f"📤 Publishing {len(pages)} pages to Webflow...\n")

for i, (node_id, keyword) in enumerate(pages, 1):
    print(f"[{i}/{len(pages)}] {keyword}...")
    subprocess.run(["python", "sei_unified.py", "publish", node_id])
    time.sleep(1)

print(f"\n✅ Done!")