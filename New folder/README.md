# Option Simulator GEX Dashboard

## What changed
- Traditional CALL/STRIKE/PUT option-chain layout on the left.
- R2, R1, Spot, S1 and S2 markers immediately to the right of the option chain.
- GEX, DEX, VEX and TEX exposure charts on the right.
- Greek selector inside the option chain: Delta, Gamma, Vega, Theta and IV.
- Live Greeks, IV Smile and IV Surface UI sections removed.
- OI and interval-over-interval change in OI are read from the option parquet files when an `oi` column exists.

## Folder setup
Update `PARQUET_BASE_PATH` and `OPTION_PARQUET_BASE_PATH` in `config_for_simulation.py`, or define them as environment variables.

## Run locally
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
python simulator.py
```
Open `http://127.0.0.1:5000`.

## Docker
```bash
docker build -t option-gex-dashboard .
docker run --rm -p 8000:8000 \
  -e PARQUET_BASE_PATH=/data \
  -e OPTION_PARQUET_BASE_PATH=/data \
  -v /your/local/data:/data \
  option-gex-dashboard
```

## GEX note
The dashboard estimates net strike GEX as call gamma exposure minus put gamma exposure using OI, lot size and spot. Validate the sign convention and scaling against your research methodology before production use.
