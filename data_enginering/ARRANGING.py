import sys
import time
import re
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ================= CONFIG =================

SOURCE_BASE = Path(
    r"C:\Users\admin\Documents\shamil\agent\agent_for_historical_data"
)

DEST_BASE = Path(
    r"C:\Users\admin\Documents\shamil\agent\weekely_data_parquet"
)

START_NUM = 147
END_NUM = 148

OPTION_ZIP_PATTERN = "NSE_OPT_TICK_*.zip"

OPTION_CSV_COLUMNS = ["date", "time", "price", "qty", "oi"]


# ================= OPTION LOGIC =================

def parse_contract(contract_name):
    name = contract_name.upper()

    m = re.search(r"^(.+?)(\d{6})(\d+)(CE|PE)$", name)
    if not m:
        return None

    underlying = m.group(1)
    expiry = m.group(2)
    strike = int(m.group(3))
    option_type = m.group(4)

    return underlying, expiry, strike, option_type


def process_option_zips(src_folder: Path, dst_folder: Path):
    output_folder = dst_folder / "NSE_OPT_TICK"
    output_folder.mkdir(parents=True, exist_ok=True)

    zip_files = sorted(src_folder.rglob(OPTION_ZIP_PATTERN))

    if not zip_files:
        print("No NSE option tick ZIP files found.")
        return

    writers = {}
    rows_written = {}

    def get_writer(key, table):
        if key in writers:
            return writers[key]

        underlying, expiry = key
        output_file = output_folder / f"{underlying}_{expiry}.parquet"

        writer = pq.ParquetWriter(
            output_file,
            table.schema,
            compression="snappy"
        )

        writers[key] = writer
        rows_written[key] = 0

        print(f"    Created option parquet: {output_file.name}", flush=True)
        return writer

    total_contracts = 0
    total_rows = 0
    skipped = 0
    start_time = time.time()

    try:
        for zip_no, zip_file in enumerate(zip_files, start=1):
            trade_date = zip_file.stem.split("_")[-1]

            print(
                f"\n  [OPTION {zip_no}/{len(zip_files)}] {zip_file.name} | Date: {trade_date}",
                flush=True
            )

            try:
                with zipfile.ZipFile(zip_file, "r") as z:
                    members = [
                        m for m in z.namelist()
                        if m.lower().endswith(".csv")
                    ]

                    print(f"  Found {len(members)} option contract CSVs")

                    start_zip = time.time()

                    for i, member in enumerate(members, start=1):
                        contract_name = Path(member).stem
                        parsed = parse_contract(contract_name)

                        if not parsed:
                            skipped += 1
                            continue

                        underlying, expiry, strike, option_type = parsed
                        key = (underlying, expiry)

                        try:
                            with z.open(member) as f:
                                df = pd.read_csv(
                                    f,
                                    header=None,
                                    names=OPTION_CSV_COLUMNS
                                )

                            if df.empty:
                                continue

                            df.insert(0, "contract", contract_name)
                            df.insert(1, "underlying", underlying)
                            df.insert(2, "expiry", expiry)
                            df.insert(3, "strike", strike)
                            df.insert(4, "option_type", option_type)

                            df["date"] = df["date"].astype("int32")
                            df["time"] = df["time"].astype("string")
                            df["price"] = df["price"].astype("float32")
                            df["qty"] = df["qty"].astype("int32")
                            df["oi"] = df["oi"].astype("int32")
                            df["strike"] = df["strike"].astype("int32")

                            table = pa.Table.from_pandas(
                                df,
                                preserve_index=False
                            )

                            writer = get_writer(key, table)
                            writer.write_table(table)

                            row_count = len(df)
                            rows_written[key] += row_count
                            total_rows += row_count
                            total_contracts += 1

                        except Exception as e:
                            print(f"ERR option CSV {zip_file.name} :: {member}: {e}")

                        if i % 100 == 0 or i == len(members):
                            elapsed = time.time() - start_zip
                            pct = (i / len(members)) * 100 if members else 0

                            print(
                                f"    {i}/{len(members)} done "
                                f"({pct:.2f}%) | current: {contract_name} | "
                                f"rows total: {total_rows:,} | "
                                f"time: {elapsed:.1f}s",
                                flush=True
                            )

            except zipfile.BadZipFile as e:
                print(f"ERR bad option zip {zip_file}: {e}")

    finally:
        print("\n  Closing option parquet writers...")
        for writer in writers.values():
            writer.close()

    elapsed = time.time() - start_time

    print("\n" + "-" * 100)
    print("OPTION PROCESS COMPLETED")
    print(f"Option contract files processed: {total_contracts}")
    print(f"Option rows written: {total_rows:,}")
    print(f"Option parquet files created: {len(writers)}")
    print(f"Skipped unknown option contracts: {skipped}")
    print(f"Option time: {elapsed:.1f} seconds")
    print("-" * 100)


# ================= OTHER ZIP LOGIC =================

def process_other_zips(src_folder: Path, dst_folder: Path):
    all_zip_files = sorted(src_folder.rglob("*.zip"))

    other_zip_files = [
        z for z in all_zip_files
        if not z.match(f"**/{OPTION_ZIP_PATTERN}")
    ]

    if not other_zip_files:
        print("No non-option ZIP files found.")
        return

    ok, fail = 0, 0

    print(f"\nFound {len(other_zip_files)} non-option ZIP file(s).")

    for zip_path in other_zip_files:
        rel_zip = zip_path.relative_to(src_folder)
        zip_out_dir = (dst_folder / rel_zip).with_suffix("")
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
                            f"OK  {rel_zip} :: {member} -> "
                            f"{out_path.relative_to(dst_folder)}"
                        )
                        ok += 1

                    except Exception as e:
                        print(f"ERR {rel_zip} :: {member}: {e}")
                        fail += 1

        except zipfile.BadZipFile as e:
            print(f"ERR {rel_zip}: bad zip ({e})")
            fail += 1

    print(f"\nOTHER ZIP PROCESS COMPLETED. Success: {ok}, Failed: {fail}")


# ================= RANGE PROCESS =================

def process_range(source_base: Path, dest_base: Path, start_num: int, end_num: int):
    source_base = Path(source_base)
    dest_base = Path(dest_base)

    if not source_base.exists():
        print(f"Source folder does not exist: {source_base}")
        return

    dest_base.mkdir(parents=True, exist_ok=True)

    for num in range(start_num, end_num + 1):
        matching_folders = [
            p for p in source_base.iterdir()
            if p.is_dir() and p.name.startswith(f"{num} ")
        ]

        if not matching_folders:
            print(f"\nNo source folder found for {num}")
            continue

        for src_folder in matching_folders:
            dst_folder = dest_base / src_folder.name
            dst_folder.mkdir(parents=True, exist_ok=True)

            print("\n" + "=" * 100)
            print(f"Processing folder: {src_folder.name}")
            print(f"Input : {src_folder}")
            print(f"Output: {dst_folder}")
            print("=" * 100)

            process_option_zips(src_folder, dst_folder)
            process_other_zips(src_folder, dst_folder)


# ================= MAIN =================

if __name__ == "__main__":
    # Optional CLI:
    # python to_parquet.py <source_base> <dest_base> <start_num> <end_num>

    if len(sys.argv) == 5:
        SOURCE_BASE = Path(sys.argv[1])
        DEST_BASE = Path(sys.argv[2])
        START_NUM = int(sys.argv[3])
        END_NUM = int(sys.argv[4])

    all_start = time.time()

    process_range(
        SOURCE_BASE,
        DEST_BASE,
        START_NUM,
        END_NUM
    )

    total_time = time.time() - all_start

    print("\n" + "=" * 100)
    print("ALL DONE")
    print(f"Range processed: {START_NUM} to {END_NUM}")
    print(f"Total time: {total_time:.1f} seconds")
    print(f"Final output base folder: {DEST_BASE}")
    print("=" * 100)