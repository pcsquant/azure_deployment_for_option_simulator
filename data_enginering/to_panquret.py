import sys
import zipfile
from pathlib import Path
import pandas as pd

# === Configure these paths ===
SOURCE_ROOT = Path(
    r"C:\Users\admin\Documents\shamil\agent\agent_for_historical_data\151 30 Mar to 03 Apr (NSE FO) - TICK"
)

DEST_ROOT = Path(
    r"C:\Users\admin\Documents\shamil\agent\parquet data\151 30 Mar to 03 Apr (NSE FO) - PARQUET"
)

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
        # Each zip becomes a folder with the same stem name under dst_root,
        # preserving the original subdirectory structure.
        zip_out_dir = (dst_root / rel_zip).with_suffix("")
        zip_out_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
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
                        df.to_parquet(out_path, engine="pyarrow",
                                      compression="snappy", index=False)
                        print(f"OK  {rel_zip} :: {member}  ->  "
                              f"{out_path.relative_to(dst_root)}")
                        ok += 1
                    except Exception as e:
                        print(f"ERR {rel_zip} :: {member}: {e}")
                        fail += 1
        except zipfile.BadZipFile as e:
            print(f"ERR {rel_zip}: bad zip ({e})")
            fail += 1

    print(f"\nDone. Success: {ok}, Failed: {fail}")


if __name__ == "__main__":
    # CLI override:  python to_panquret.py <source> <output>
    if len(sys.argv) == 3:
        SOURCE_ROOT = Path(sys.argv[1])
        DEST_ROOT = Path(sys.argv[2])

    convert_zipped_csvs_to_parquet(SOURCE_ROOT, DEST_ROOT)