from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import StreamingResponse
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

def split_header(header: str) -> str:
    return "\n".join(header.split())

def calculate_column_width(series: pd.Series, header: str) -> int:
    strings = series.dropna().astype(str)
    max_content_len = strings.map(len).max() if not strings.empty else 0
    max_len = max(max_content_len, len(header))
    return min(60, int(max_len * 1.2) + 2)

def process_excel_file(input_df: pd.DataFrame, region: str):
    desired_final_columns = [
        'MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE',
        'TOTAL LOGGING DAYS', 'TOTAL TRANSITION COUNT',
        'TOTAL EKYC SUCCESS', 'TOTAL APY SUCCESS',
        'TOTAL PMSBY SUCCESS', 'TOTAL PMJJBY SUCCESS',
        'TOTAL LOAN RECOVERY', 'TOTAL AMOUNT',
        'LOAN LEAD GENERATION COUNT', 'CO ORDINATOR NAME'
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

    eff_days = 0
    if 'TOTAL LOGGING DAYS' in df_filtered:
        df_filtered['TOTAL LOGGING DAYS'] = pd.to_numeric(df_filtered['TOTAL LOGGING DAYS'], errors='coerce')
        c = df_filtered['TOTAL LOGGING DAYS'].dropna()
        eff_days = c.max() if not c.empty else 0
    if eff_days == 0:
        eff_days = BASE_MONTH_DAYS

    calculated_dynamic_targets = {
        col: math.ceil((mt / BASE_MONTH_DAYS) * eff_days)
        for col, mt in monthly_targets.items() if col in df_filtered.columns
    }

    df_inactive = df_filtered[df_filtered['TOTAL LOGGING DAYS'] == 0].copy()

    kpi_cols = [col for col in monthly_targets if col in df_filtered.columns]
    df_filtered[kpi_cols] = df_filtered[kpi_cols].apply(pd.to_numeric, errors='coerce')
    df_filtered = df_filtered[~(df_filtered[kpi_cols].fillna(0) == 0).all(axis=1)]

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    out_path = tmp.name
    tmp.close()
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
    workbook = writer.book

    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#FFFF00', 'font_color': '#000000', 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'border': 1})
    db_fmt = workbook.add_format({'font_color': '#000000', 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    red_fmt = workbook.add_format({'font_color': '#FF0000', 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#000000', 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    red_bg_black = workbook.add_format({'bg_color': '#FF0000', 'font_color': '#000000', 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})

    def write_sheet(name, df_s, color_col=None, color_fmt=None, header_format=header_fmt, text_fmt=None):
        df_s.to_excel(writer, sheet_name=name, index=False, header=False)
        ws = writer.sheets[name]
        headers = [split_header(c) for c in df_s.columns]
        ws.write_row(0, 0, headers, header_format)
        ws.set_row(0, 40)

        for r in range(len(df_s)):
            for c_i, col in enumerate(df_s.columns):
                val = df_s.iloc[r, c_i]
                fmt = color_fmt if color_col == col and color_fmt else (text_fmt or db_fmt)
                ws.write(r + 1, c_i, "" if pd.isna(val) else val, fmt)

        for c_i, col in enumerate(df_s.columns):
            width = calculate_column_width(df_s[col], col)
            ws.set_column(c_i, c_i, width)

        ws.write(len(df_s) + 1, 0, "Total Count", header_format)
        ws.write(len(df_s) + 1, 1, len(df_s), header_format)

    df_processed = df_filtered.copy()
    if 'CO ORDINATOR NAME' in df_processed.columns:
        df_processed.drop(columns=['CO ORDINATOR NAME'], inplace=True)

    df_processed = df_processed.sort_values(by='TOTAL TRANSITION COUNT', ascending=False)
    df_processed.to_excel(writer, sheet_name='Processed', index=False, header=False)
    ws0 = writer.sheets['Processed']
    headers0 = [split_header(c) for c in df_processed.columns]
    ws0.write_row(0, 0, headers0, header_fmt)
    ws0.set_row(0, 40)

    for r in range(len(df_processed)):
        for c_i, col in enumerate(df_processed.columns):
            val = df_processed.iloc[r, c_i]
            fmt = db_fmt
            if col in calculated_dynamic_targets:
                num = pd.to_numeric(val, errors='coerce')
                fmt = red_fmt if pd.isna(num) or num < calculated_dynamic_targets[col] else green_fmt
            if col == 'TOTAL AMOUNT':
                num = pd.to_numeric(val, errors='coerce')
                if pd.isna(num) or num == 0:
                    fmt = red_fmt
            ws0.write(r + 1, c_i, "" if pd.isna(val) else val, fmt)

    for c_i, col in enumerate(df_processed.columns):
        ws0.set_column(c_i, c_i, calculate_column_width(df_processed[col], col))

    ws0.write(len(df_processed) + 2, 0, "Achieved Count", header_fmt)
    ws0.write(len(df_processed) + 3, 0, "Not Achieved Count", header_fmt)
    for c_i, col in enumerate(df_processed.columns):
        if col in calculated_dynamic_targets:
            series = pd.to_numeric(df_processed[col], errors='coerce')
            ws0.write(len(df_processed) + 2, c_i, int((series >= calculated_dynamic_targets[col]).sum()), green_fmt)
            ws0.write(len(df_processed) + 3, c_i, int((series < calculated_dynamic_targets[col]).sum()), red_fmt)

    inactive_cols_base = ['MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE', 'TOTAL LOGGING DAYS', 'CO ORDINATOR NAME']
    inactive_cols = [col for col in inactive_cols_base if col in df_inactive.columns]
    if not df_inactive.empty and inactive_cols:
        write_sheet('Inactive', df_inactive[inactive_cols], text_fmt=red_fmt)

    if {'TOTAL LOGGING DAYS', 'TOTAL TRANSITION COUNT'}.issubset(df_filtered.columns):
        dft = df_filtered[df_filtered['TOTAL LOGGING DAYS'] > 0].copy()
        dft['TARGET'] = dft['TOTAL LOGGING DAYS'].apply(lambda x: math.ceil((100 / 31) * x))
        dft = dft[dft['TOTAL TRANSITION COUNT'] < dft['TARGET']]
        if not dft.empty:
            write_sheet('Low_Trans', dft[['MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE', 'TOTAL LOGGING DAYS', 'TOTAL TRANSITION COUNT', 'CO ORDINATOR NAME']], color_col='TOTAL TRANSITION COUNT', color_fmt=red_bg_black)

    dfr = df_filtered[df_filtered['TOTAL LOAN RECOVERY'] > 0]
    if not dfr.empty:
        write_sheet('Recovery_List', dfr[['MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE', 'TOTAL LOAN RECOVERY', 'TOTAL AMOUNT', 'CO ORDINATOR NAME']])

    dfl = df_filtered[df_filtered['LOAN LEAD GENERATION COUNT'] > 0]
    if not dfl.empty:
        write_sheet('Loan_Lead_List', dfl[['MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE', 'LOAN LEAD GENERATION COUNT', 'CO ORDINATOR NAME']])

    df_pm = df_filtered[
        (df_filtered['TOTAL LOGGING DAYS'] > 0) &
        (df_filtered['TOTAL APY SUCCESS'].fillna(0) == 0) &
        (df_filtered['TOTAL PMSBY SUCCESS'].fillna(0) == 0) &
        (df_filtered['TOTAL PMJJBY SUCCESS'].fillna(0) == 0)
    ]
    if not df_pm.empty:
        write_sheet('PM_Not_Working', df_pm[['MECHNAT_ID', 'BC_NAME', 'BRANCH_NAME', 'LOCATION TYPE', 'TOTAL LOGGING DAYS', 'TOTAL APY SUCCESS', 'TOTAL PMSBY SUCCESS', 'TOTAL PMJJBY SUCCESS', 'CO ORDINATOR NAME']], text_fmt=red_fmt)

    writer.close()
    return out_path

@app.post("/process-single-region/")
async def process_single_region(
    region: RegionEnum = Form(...),
    file: UploadFile = File(...)
):
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail="Only Excel files allowed")

    temp_dir = tempfile.mkdtemp()
    input_path = os.path.join(temp_dir, file.filename)

    async with aiofiles.open(input_path, 'wb') as out_file:
        content = await file.read()
        await out_file.write(content)

    try:
        df = pd.read_excel(input_path, sheet_name="DATA", header=0)
        output_file_path = process_excel_file(df, region.value)

        async def file_streamer():
            async with aiofiles.open(output_file_path, 'rb') as f:
                while True:
                    chunk = await f.read(8192)
                    if not chunk:
                        break
                    yield chunk
            shutil.rmtree(temp_dir)

        return StreamingResponse(file_streamer(),
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers={"Content-Disposition": f"attachment; filename={region.value}_processed.xlsx"})

    except Exception as e:
        shutil.rmtree(temp_dir)
        raise HTTPException(500, detail=str(e))
