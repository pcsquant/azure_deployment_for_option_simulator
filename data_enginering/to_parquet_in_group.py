import sys
import zipfile
from pathlib import Path
import pandas as pd


# === Configure base folders ===
SOURCE_BASE = Path(
    r"C:\Users\admin\Documents\shamil\agent\agent_for_historical_data"
)

DEST_BASE = Path(
    r"C:\Users\admin\Documents\shamil\agent\parquet data"
)

START_NUM = 154
END_NUM = 153


def convert_zipped_csvs_to_parquet(src_root: Path, dst_root: Path):
    src_root = Path(src_root)
    dst_root = Path(dst_root)

    if not src_root.exists():
        print(f"Source folder does not exist: {src_root}")
        return

    dst_root.mkdir(parents=True, exist_ok=True)

    zip_files = list(src_root.rglob("*.zip"))
    if not zip_files:
        print(f"No ZIP files found under: {src_root}")
        return

    print(f"Found {len(zip_files)} ZIP file(s).")
    print(f"Source : {src_root}")
    print(f"Output : {dst_root}\n")

    ok, fail = 0, 0

    for zip_path in zip_files:
        rel_zip = zip_path.relative_to(src_root)

        zip_out_dir = (dst_root / rel_zip).with_suffix("")
        zip_out_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_members = [
                    m for m in zf.namelist()
                    if m.lower().endswith(".csv")
                ]

                if not csv_members:
                    print(f"SKIP {rel_zip}: no CSVs inside")
                    continue

                for member in csv_members:
                    member_path = Path(member)
                    out_path = zip_out_dir / member_path.with_suffix(".parquet")
                    out_path.parent.mkdir(parents=True, exist_ok=True)

                    try:
                        with zf.open(member) as f:
                            df = pd.read_csv(f)

                        df.to_parquet(
                            out_path,
                            engine="pyarrow",
                            compression="snappy",
                            index=False
                        )

                        print(
                            f"OK  {rel_zip} :: {member}  ->  "
                            f"{out_path.relative_to(dst_root)}"
                        )
                        ok += 1

                    except Exception as e:
                        print(f"ERR {rel_zip} :: {member}: {e}")
                        fail += 1

        except zipfile.BadZipFile as e:
            print(f"ERR {rel_zip}: bad zip ({e})")
            fail += 1

    print(f"\nDone. Success: {ok}, Failed: {fail}")


def process_range(source_base: Path, dest_base: Path, start_num: int, end_num: int):
    for num in range(start_num, end_num + 1):
        matching_folders = [
            p for p in source_base.iterdir()
            if p.is_dir() and p.name.startswith(f"{num} ")
        ]

        if not matching_folders:
            print(f"\nNo source folder found for {num}")
            continue

        for src_folder in matching_folders:
            dst_folder_name = src_folder.name.replace(" - TICK", " - PARQUET")
            dst_folder = dest_base / dst_folder_name

            print("\n" + "=" * 80)
            print(f"Processing folder: {src_folder.name}")
            print("=" * 80)

            convert_zipped_csvs_to_parquet(src_folder, dst_folder)


if __name__ == "__main__":
    # Optional CLI:
    # python to_parquet.py <source_base> <dest_base> <start_num> <end_num>
    if len(sys.argv) == 5:
        SOURCE_BASE = Path(sys.argv[1])
        DEST_BASE = Path(sys.argv[2])
        START_NUM = int(sys.argv[3])
        END_NUM = int(sys.argv[4])

    process_range(SOURCE_BASE, DEST_BASE, START_NUM, END_NUM)