from pathlib import Path
import zipfile
import time
import re
import pandas as pd

SOURCE_WEEK_FOLDER = Path(
    r"C:\Users\admin\Documents\shamil\agent\agent_for_historical_data\151 30 Mar to 03 Apr (NSE FO) - TICK"
)

OUTPUT_BASE_FOLDER = Path(
    r"C:\Users\admin\Documents\shamil\agent\weekely_data"
)

# Automatically create an output folder with the same name as the source folder
OUTPUT_WEEK_FOLDER = OUTPUT_BASE_FOLDER / SOURCE_WEEK_FOLDER.name

OUTPUT_OPT_FOLDER = OUTPUT_WEEK_FOLDER / "OPT_TICK"
OUTPUT_FUT_FOLDER = OUTPUT_WEEK_FOLDER / "FUT_TICK"
OUTPUT_IDX_FOLDER = OUTPUT_WEEK_FOLDER / "IDX_TICK"

OUTPUT_OPT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_FUT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_IDX_FOLDER.mkdir(parents=True, exist_ok=True)

CSV_COLUMNS = ["date", "time", "price", "qty", "oi"]


# =====================================================
# CONTRACT PARSER
# =====================================================

def parse_option_contract(contract_name):
    """
    Supports:
    NIFTY26033022000CE
    ASHOKLEY260330147.5PE
    ONGC260330209.75CE
    """
    name = contract_name.upper()

    m = re.search(r"^(.+?)(\d{6})(\d+(?:\.\d+)?)(CE|PE)$", name)

    if not m:
        return None

    underlying = m.group(1)
    expiry = m.group(2)
    strike = float(m.group(3))
    side = m.group(4)

    return underlying, expiry, strike, side


def parse_future_contract(contract_name):
    """
    Supports:
    AARTIIND25JULFUT
    ABB25JUNFUT
    RELIANCE25MAYFUT
    BANKNIFTY25JUNFUT
    BAJAJ-AUTO25MAYFUT
    M&M25JUNFUT
    """

    name = contract_name.upper()

    m = re.search(
        r"^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$",
        name
    )

    if not m:
        return None

    underlying = m.group(1)
    expiry = m.group(2) + m.group(3)

    return underlying, expiry


# =====================================================
# READ CSV
# =====================================================

def read_tick_csv(src):
    df = pd.read_csv(
        src,
        header=None,
        engine="c"
    )

    if df.empty:
        return df

    if df.shape[1] >= 5:
        df = df.iloc[:, :5]
        df.columns = CSV_COLUMNS

    elif df.shape[1] == 3:
        df.columns = ["date", "time", "price"]
        df["qty"] = pd.NA
        df["oi"] = pd.NA
        df = df[CSV_COLUMNS]

    else:
        raise ValueError(f"Unexpected column count: {df.shape[1]}")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce")

    return df[CSV_COLUMNS]


# =====================================================
# PROCESS SEGMENT
# =====================================================

def process_segment(
    segment_name,
    zip_pattern,
    output_folder,
    parser=None,
    required_folder=None
):
    zip_files = sorted(SOURCE_WEEK_FOLDER.glob(zip_pattern))

    print("\n" + "=" * 80)
    print(f"PROCESSING: {segment_name}")
    print(f"Found {len(zip_files)} zip files")
    print(f"Input : {SOURCE_WEEK_FOLDER}")
    print(f"Output: {output_folder}")
    print("=" * 80)

    instrument_data = {}
    skipped_files = []

    total_files = 0
    total_rows = 0

    segment_start = time.time()

    for zip_no, zip_file in enumerate(zip_files, start=1):
        trade_date = zip_file.stem.split("_")[-1]

        print(
            f"\n[{zip_no}/{len(zip_files)}] Opening {zip_file.name} | Date: {trade_date}",
            flush=True
        )

        start_zip = time.time()

        with zipfile.ZipFile(zip_file, "r") as z:
            members = [
                m for m in z.namelist()
                if m.lower().endswith(".csv")
            ]

            # For FUT_TICK, use only Contract Futures
            if required_folder is not None:
                members = [
                    m for m in members
                    if required_folder.lower() in m.lower()
                ]

            total_members = len(members)

            print(f"Found {total_members} CSV files", flush=True)

            for i, member in enumerate(members, start=1):
                instrument_name = Path(member).stem

                if parser is not None:
                    parsed = parser(instrument_name)

                    if not parsed:
                        skipped_files.append((instrument_name, "Name format not matched"))
                        print(f"Skipping unknown format: {instrument_name}")
                        continue

                try:
                    with z.open(member) as src:
                        df = read_tick_csv(src)

                    if df.empty:
                        continue

                    instrument_data.setdefault(instrument_name, []).append(df)

                    total_files += 1
                    total_rows += len(df)

                except Exception as e:
                    skipped_files.append((instrument_name, str(e)))
                    print(f"Skipping {instrument_name} | Error: {e}")

                if i % 100 == 0 or i == total_members:
                    elapsed = time.time() - start_zip
                    pct = (i / total_members) * 100 if total_members else 0

                    print(
                        f"  {i}/{total_members} done "
                        f"({pct:.2f}%) | current: {instrument_name} | "
                        f"rows so far: {total_rows:,} | "
                        f"time: {elapsed:.1f}s",
                        flush=True
                    )

    print("\nWriting Parquet files...")
    print("-" * 80)

    for i, (instrument_name, df_list) in enumerate(sorted(instrument_data.items()), start=1):
        final_df = pd.concat(df_list, ignore_index=True)

        final_df = final_df[CSV_COLUMNS]

        output_file = output_folder / f"{instrument_name}.parquet"

        final_df.to_parquet(
            output_file,
            engine="pyarrow",
            compression="zstd",
            index=False
        )

        if i % 100 == 0 or i == len(instrument_data):
            print(
                f"  {i}/{len(instrument_data)} parquet saved | "
                f"current: {instrument_name} | rows: {len(final_df):,}",
                flush=True
            )

    if skipped_files:
        skipped_file = output_folder / f"skipped_{segment_name}.txt"

        with open(skipped_file, "w", encoding="utf-8") as f:
            for name, error in skipped_files:
                f.write(f"{name} | {error}\n")

        print(f"\nSkipped log saved: {skipped_file}")

    segment_time = time.time() - segment_start

    print("\n" + "-" * 80)
    print(f"{segment_name} DONE")
    print(f"ZIP files processed   : {len(zip_files)}")
    print(f"CSV files processed   : {total_files}")
    print(f"Total rows processed  : {total_rows:,}")
    print(f"Parquet files created : {len(instrument_data)}")
    print(f"Skipped files         : {len(skipped_files)}")
    print(f"Time taken            : {segment_time:.1f} seconds")
    print(f"Final folder          : {output_folder}")
    print("-" * 80)


# =====================================================
# MAIN
# =====================================================

start_all = time.time()

process_segment(
    segment_name="OPT_TICK",
    zip_pattern="NSE_OPT_TICK_*.zip",
    output_folder=OUTPUT_OPT_FOLDER,
    parser=parse_option_contract
)

process_segment(
    segment_name="FUT_TICK",
    zip_pattern="NSE_FUT_TICK_*.zip",
    output_folder=OUTPUT_FUT_FOLDER,
    parser=parse_future_contract,
    required_folder="Contract Futures"
)

process_segment(
    segment_name="IDX_TICK",
    zip_pattern="NSE_IDX_TICK_*.zip",
    output_folder=OUTPUT_IDX_FOLDER,
    parser=None
)

total_time = time.time() - start_all

print("\n" + "=" * 80)
print("ALL DONE")
print(f"Total time   : {total_time:.1f} seconds")
print(f"Final folder : {OUTPUT_WEEK_FOLDER}")
print("=" * 80)