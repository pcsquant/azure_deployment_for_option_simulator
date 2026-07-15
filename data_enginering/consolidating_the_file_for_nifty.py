from pathlib import Path
import zipfile
import time
import re
import pandas as pd

# ================= CONFIG =================

BASE_FOLDER = Path(
    r"Y:\SYMPHONY\Historical Data\truedata\NSE-FO Tick IEOD"
)

OUTPUT_BASE_FOLDER = Path(
    r"C:\Users\admin\Documents\shamil\agent\weekely_data"
)

WEEK_START = 154
WEEK_END = 165

CSV_COLUMNS = ["date", "time", "price", "qty", "oi"]

ALLOWED_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "INDIAVIX"}


# =====================================================
# FIND WEEK FOLDERS 152 TO 165
# =====================================================

def find_week_folders():
    folders = []

    for folder in BASE_FOLDER.iterdir():
        if folder.is_dir():
            m = re.match(r"^(\d+)\s+", folder.name)
            if m:
                week_no = int(m.group(1))

                if WEEK_START <= week_no <= WEEK_END:
                    folders.append(folder)

    return sorted(folders, key=lambda x: int(re.match(r"^(\d+)", x.name).group(1)))


# =====================================================
# CONTRACT PARSERS
# =====================================================

def parse_option_contract(contract_name):
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


def parse_index_contract(contract_name):
    name = contract_name.upper()

    if name in ALLOWED_UNDERLYINGS:
        return name

    return None


# =====================================================
# READ CSV
# =====================================================

def read_tick_csv(src):
    df = pd.read_csv(src, header=None, engine="c")

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
# PROCESS ONE SEGMENT
# =====================================================

def process_segment(
    source_week_folder,
    segment_name,
    zip_pattern,
    output_folder,
    parser=None,
    required_folder=None
):
    zip_files = sorted(source_week_folder.glob(zip_pattern))

    print("\n" + "=" * 80)
    print(f"PROCESSING: {segment_name}")
    print(f"Week folder: {source_week_folder.name}")
    print(f"Found {len(zip_files)} zip files")
    print(f"Output: {output_folder}")
    print("=" * 80)

    output_folder.mkdir(parents=True, exist_ok=True)

    instrument_data = {}
    skipped_files = []

    total_files = 0
    total_rows = 0

    for zip_no, zip_file in enumerate(zip_files, start=1):
        trade_date = zip_file.stem.split("_")[-1]

        print(f"\n[{zip_no}/{len(zip_files)}] Opening {zip_file.name} | Date: {trade_date}")

        with zipfile.ZipFile(zip_file, "r") as z:
            members = [m for m in z.namelist() if m.lower().endswith(".csv")]

            if required_folder is not None:
                members = [
                    m for m in members
                    if required_folder.lower() in m.lower()
                ]

            for i, member in enumerate(members, start=1):
                instrument_name = Path(member).stem.upper()

                if parser is not None:
                    parsed = parser(instrument_name)

                    if not parsed:
                        skipped_files.append((instrument_name, "format not matched"))
                        continue

                    if segment_name in ["OPT_TICK", "FUT_TICK"]:
                        underlying = parsed[0]
                    elif segment_name == "IDX_TICK":
                        underlying = parsed
                    else:
                        underlying = None

                    if underlying not in ALLOWED_UNDERLYINGS:
                        skipped_files.append((instrument_name, "Other underlying skipped"))
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

                if i % 100 == 0 or i == len(members):
                    print(
                        f"  {i}/{len(members)} done | "
                        f"current: {instrument_name} | "
                        f"rows so far: {total_rows:,}"
                    )

    print("\nWriting Parquet files...")

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
                f"  {i}/{len(instrument_data)} saved | "
                f"{instrument_name} | rows: {len(final_df):,}"
            )

    print("\n" + "-" * 80)
    print(f"{segment_name} DONE")
    print(f"CSV files processed   : {total_files}")
    print(f"Total rows processed  : {total_rows:,}")
    print(f"Parquet files created : {len(instrument_data)}")
    print(f"Skipped files         : {len(skipped_files)}")
    print(f"Final folder          : {output_folder}")
    print("-" * 80)


# =====================================================
# MAIN
# =====================================================

start_all = time.time()

week_folders = find_week_folders()

print(f"Found week folders: {len(week_folders)}")
for f in week_folders:
    print(f"  {f.name}")

for SOURCE_WEEK_FOLDER in week_folders:

    OUTPUT_WEEK_FOLDER = OUTPUT_BASE_FOLDER / SOURCE_WEEK_FOLDER.name

    OUTPUT_OPT_FOLDER = OUTPUT_WEEK_FOLDER / "OPT_TICK"
    OUTPUT_FUT_FOLDER = OUTPUT_WEEK_FOLDER / "FUT_TICK"
    OUTPUT_IDX_FOLDER = OUTPUT_WEEK_FOLDER / "IDX_TICK"

    print("\n" + "#" * 100)
    print(f"STARTING WEEK: {SOURCE_WEEK_FOLDER.name}")
    print("#" * 100)

    process_segment(
        source_week_folder=SOURCE_WEEK_FOLDER,
        segment_name="OPT_TICK",
        zip_pattern="NSE_OPT_TICK_*.zip",
        output_folder=OUTPUT_OPT_FOLDER,
        parser=parse_option_contract
    )

    process_segment(
        source_week_folder=SOURCE_WEEK_FOLDER,
        segment_name="FUT_TICK",
        zip_pattern="NSE_FUT_TICK_*.zip",
        output_folder=OUTPUT_FUT_FOLDER,
        parser=parse_future_contract,
        required_folder="Contract Futures"
    )

    process_segment(
        source_week_folder=SOURCE_WEEK_FOLDER,
        segment_name="IDX_TICK",
        zip_pattern="NSE_IDX_TICK_*.zip",
        output_folder=OUTPUT_IDX_FOLDER,
        parser=parse_index_contract
    )

total_time = time.time() - start_all

print("\n" + "=" * 100)
print("ALL WEEKS DONE")
print(f"Weeks processed : {WEEK_START} to {WEEK_END}")
print(f"Total time      : {total_time:.1f} seconds")
print(f"Final folder    : {OUTPUT_BASE_FOLDER}")
print("=" * 100)