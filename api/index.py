from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import StreamingResponse, HTMLResponse
from pathlib import Path
import os
import aiofiles
import pandas as pd
import math
import tempfile
import shutil
from enum import Enum

app = FastAPI()
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

class RegionEnum(str, Enum):
    INDORE = "INDORE"
    NARMADAPURAM = "NARMADAPURAM"
    REWA = "REWA"
    BHOPALCENTRAL = "BHOPALCENTRAL"
    GWALIOR = "GWALIOR"
    JABALPUR = "JABALPUR"

REGIONS = [region.value for region in RegionEnum]

@app.get("/", response_class=HTMLResponse)
async def form_page():
    region_options = "\n".join([f'<option value="{r}">{r}</option>' for r in REGIONS])
    return f"""
    <html>
        <head><title>Upload Excel File</title></head>
        <body>
            <h2>Upload Excel File for a Region</h2>
            <form action="/process-single-region/" enctype="multipart/form-data" method="post">
                <label for="region">Select Region:</label>
                <select name="region">{region_options}</select><br><br>
                <label for="file">Upload Excel File:</label>
                <input type="file" name="file" accept=".xlsx,.xls" required/><br><br>
                <input type="submit" value="Process"/>
            </form>
        </body>
    </html>
    """

def split_header(header: str) -> str:
    return "\n".join(header.split())

def calculate_column_width(series: pd.Series, header: str) -> int:
    strings = series.dropna().astype(str)
    max_content_len = strings.map(len).max() if not strings.empty else 0
    max_len = max(max_content_len, len(header))
    return min(70, int(max_len * 1.15) + 2)

def process_excel_file(input_df: pd.DataFrame, region: str):
    desired_final_columns = [
        'MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE',
        'TOTAL LOGGING DAYS', 'TOTAL TRANSITION COUNT',
        'TOTAL EKYC SUCCESS', 'TOTAL APY SUCCESS',
        'TOTAL PMSBY SUCCESS', 'TOTAL PMJJBY SUCCESS',
        'TOTAL LOAN RECOVERY', 'TOTAL AMOUNT',
        'LOAN LEAD GENERATION COUNT'
    ]

    column_renaming_map = {'TOTAL_FIN_SUCCESS': 'TOTAL TRANSITION COUNT'}
    monthly_targets = {
        'TOTAL LOGGING DAYS': 24, 'TOTAL TRANSITION COUNT': 200,
        'TOTAL EKYC SUCCESS': 15, 'TOTAL APY SUCCESS': 5,
        'TOTAL PMSBY SUCCESS': 30, 'TOTAL PMJJBY SUCCESS': 15,
        'TOTAL LOAN RECOVERY': 1, 'LOAN LEAD GENERATION COUNT': 1
    }
    BASE_MONTH_DAYS = 31

    df = input_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for o, n in column_renaming_map.items():
        if o in df:
            df.rename(columns={o: n}, inplace=True)

    if 'REGION_NAME' in df.columns:
        df = df[df['REGION_NAME'].astype(str).str.upper() == region.upper()]
        df.drop(columns=['REGION_NAME'], inplace=True)
    else:
        raise ValueError("Missing REGION_NAME column")

    df_filtered = df[[col for col in desired_final_columns if col in df.columns]].copy()

    eff_days = df_filtered['TOTAL LOGGING DAYS'].dropna().max() if 'TOTAL LOGGING DAYS' in df_filtered else 0
    eff_days = eff_days if eff_days else BASE_MONTH_DAYS

    calculated_dynamic_targets = {
        col: math.ceil((mt / BASE_MONTH_DAYS) * eff_days)
        for col, mt in monthly_targets.items() if col in df_filtered
    }

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    out_path = tmp.name
    tmp.close()
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
    workbook = writer.book
    fmt_header = workbook.add_format({'bold': True, 'bg_color': '#FFFF00', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
    fmt_red = workbook.add_format({'font_color': '#FF0000', 'border': 1})
    fmt_green = workbook.add_format({'bg_color': '#C6EFCE', 'border': 1})
    fmt_default = workbook.add_format({'border': 1})
    fmt_red_bg = workbook.add_format({'bg_color': '#FF0000', 'font_color': '#000000', 'border': 1})

    def write_sheet(name, df_s, highlight_col=None, highlight_fmt=None):
        df_s.to_excel(writer, sheet_name=name, index=False, header=False)
        ws = writer.sheets[name]
        headers = [split_header(c) for c in df_s.columns]
        ws.write_row(0, 0, headers, fmt_header)
        for r in range(len(df_s)):
            for c_i, col in enumerate(df_s.columns):
                val = df_s.iloc[r, c_i]
                if highlight_col == col:
                    fmt = highlight_fmt
                elif col in calculated_dynamic_targets:
                    v = pd.to_numeric(val, errors='coerce')
                    fmt = fmt_green if v >= calculated_dynamic_targets[col] else fmt_red
                else:
                    fmt = fmt_default
                ws.write(r + 1, c_i, "" if pd.isna(val) else val, fmt)
        for c_i, col in enumerate(df_s.columns):
            width = calculate_column_width(df_s[col], col)
            ws.set_column(c_i, c_i, width)

    write_sheet("Processed", df_filtered)

    inactive = df_filtered[df_filtered["TOTAL LOGGING DAYS"] == 0] if "TOTAL LOGGING DAYS" in df_filtered else pd.DataFrame()
    if not inactive.empty:
        write_sheet("Inactive", inactive)

    low_trans = df_filtered[
        (df_filtered["TOTAL LOGGING DAYS"] > 0) &
        (df_filtered["TOTAL TRANSITION COUNT"] < math.ceil((100 / 31) * df_filtered["TOTAL LOGGING DAYS"]))
    ] if "TOTAL LOGGING DAYS" in df_filtered and "TOTAL TRANSITION COUNT" in df_filtered else pd.DataFrame()
    if not low_trans.empty:
        write_sheet("Low_Trans", low_trans, "TOTAL TRANSITION COUNT", fmt_red_bg)

    writer.close()
    return out_path

@app.post("/process-single-region/")
async def process_single_region(
    region: RegionEnum = Form(...),
    file: UploadFile = File(...)
):
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail="Only Excel files (.xlsx, .xls) allowed")

    temp_dir = tempfile.mkdtemp()
    input_path = os.path.join(temp_dir, file.filename)

    async with aiofiles.open(input_path, 'wb') as out_file:
        content = await file.read()
        await out_file.write(content)

    try:
        df = pd.read_excel(input_path, sheet_name="DATA", header=0)
        output_file_path = process_excel_file(df, region.value)

        async def stream():
            async with aiofiles.open(output_file_path, 'rb') as f:
                while True:
                    chunk = await f.read(8192)
                    if not chunk:
                        break
                    yield chunk
            shutil.rmtree(temp_dir)

        return StreamingResponse(
            stream(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={region.value}_processed.xlsx"}
        )

    except Exception as e:
        shutil.rmtree(temp_dir)
        raise HTTPException(500, detail=str(e))
