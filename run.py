from flask import Flask, request, render_template, send_file
import pandas as pd
import datetime
import os

app = Flask(__name__, template_folder='templates')

# Ensure the uploads and downloads directories exist
os.makedirs('uploads', exist_ok=True)
os.makedirs('downloads', exist_ok=True)

# Define the logic for calculating the "No of Units" column
def calculate_units(crew_count):
    return (crew_count - 1) // 31 + 1 if crew_count > 0 else 0

# Define the custom grouping logic function
def custom_group_buildings(df, specific_groupings):
    df_custom_grouped_list = []
    for time in df['TIME'].unique():
        df_time = df[df['TIME'] == time]
        for combined_name, buildings in specific_groupings.items():
            valid_buildings = df_time[df_time['TO'].isin(buildings) & (df_time['CREW'] > 0)]
            if not valid_buildings.empty:
                valid_combined_name = ' & '.join(valid_buildings['TO'])
                crew_sum = valid_buildings['CREW'].sum()
                df_custom_grouped_list.append({
                    'TIME': time,
                    'TO': valid_combined_name,
                    'CREW': crew_sum,
                    'NO OF UNITS': calculate_units(crew_sum)
                })
        other_buildings = df_time[~df_time['TO'].isin(sum(specific_groupings.values(), ()))]
        other_buildings['NO OF UNITS'] = other_buildings['CREW'].apply(calculate_units)
        df_custom_grouped_list.extend(other_buildings.to_dict(orient='records'))
    df_custom_grouped = pd.DataFrame(df_custom_grouped_list).drop_duplicates(subset=['TIME', 'TO'])
    return df_custom_grouped

@app.route('/')
def home():
    print("Home route accessed")
    return render_template('upload.html')

@app.route('/uploader', methods=['GET', 'POST'])
def uploader():
    print("Uploader route accessed")
    if request.method == 'POST':
        inbound_file = request.files['inbound']
        outbound_file = request.files['outbound']
        
        if inbound_file and outbound_file:
            print("Files received")
            inbound_file_path = os.path.join('uploads', inbound_file.filename)
            outbound_file_path = os.path.join('uploads', outbound_file.filename)
            inbound_file.save(inbound_file_path)
            outbound_file.save(outbound_file_path)

            df_inbound = pd.read_excel(inbound_file_path)
            df_outbound = pd.read_excel(outbound_file_path)

            print("Inbound DataFrame columns:", df_inbound.columns)
            print("Outbound DataFrame columns:", df_outbound.columns)

            # Rename the 'TIME - 1' column to 'TIME' in the inbound DataFrame
            df_inbound.rename(columns={'TIME - 1': 'TIME'}, inplace=True)

            # Process Inbound Data
            try:
                df_inbound_melted = df_inbound.melt(id_vars=["TIME"], var_name="TO", value_name="CREW")
                df_inbound_melted = df_inbound_melted.dropna(subset=["CREW"])
                df_inbound_melted['CREW'] = pd.to_numeric(df_inbound_melted['CREW'], errors='coerce').fillna(0)
            except KeyError as e:
                return f"KeyError: {e}. Available columns are: {df_inbound.columns}"

            specific_groupings = {
                "EM6 TALAL QAMZI": ("Talal", "Al Qamzi", "EM6"),
                "SARAB SAFA": ("SARAB", "SAFA"),
                "SABREEN FALTAT": ("SAB", "FT"),
                "DR.KHALIFA & TECOM 1,2": ("Dr. K", "TECOM")
            }

            df_custom_grouped_inbound = custom_group_buildings(df_inbound_melted, specific_groupings)

            name_mapping = {
                "Talal": "TALAL",
                "Al Qamzi": "QAMZI",
                "EM6": "EM6",
                "Dr. K": "DR.KHALIFA",
                "TECOM": "TECOM 1,2",
                "GCT": "GROSVENOUR",
                "PINK": "PINK",
                "SAB": "SABREEN",
                "FT": "FALTAT",
                "MD": "MANAZIL DIERA",
                "MT": "MANAZIL TOWER",
                "SON": "SONBOULAH",
                "GT": "GARHOUD TOWERS",
                "MIT": "MILLINUM TOWER",
                "PZABEEL": "PARK ZABEEL 1,2",
            }

            df_custom_grouped_inbound['TO'] = df_custom_grouped_inbound['TO'].apply(lambda x: '  '.join(name_mapping.get(item, item) for item in x.split(' & ')))

            # Use the correct format for TIME column
            df_custom_grouped_inbound['TIME'] = df_custom_grouped_inbound['TIME'].apply(lambda x: x.strftime('%H:%M') if not pd.isnull(x) else '')

            tomorrows_date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%d-%b')
            df_custom_grouped_inbound['DATE'] = tomorrows_date
            df_custom_grouped_inbound['FROM'] = df_custom_grouped_inbound['TO']
            df_custom_grouped_inbound['TO'] = 'EAC-C'
            df_final_inbound = df_custom_grouped_inbound[['DATE', 'NO OF UNITS', 'TIME', 'FROM', 'TO', 'CREW']].sort_values(by='TIME')
            inbound_output_path = os.path.join('downloads', 'cc_trip_rearranged_inbound.xlsx')
            df_final_inbound.to_excel(inbound_output_path, index=False)

            # Process Outbound Data (similar to inbound)
            try:
                df_outbound_melted = df_outbound.melt(id_vars=["TIME"], var_name="TO", value_name="CREW")
                df_outbound_melted = df_outbound_melted.dropna(subset=["CREW"])
                df_outbound_melted['CREW'] = pd.to_numeric(df_outbound_melted['CREW'], errors='coerce').fillna(0)
            except KeyError as e:
                return f"KeyError: {e}. Available columns are: {df_outbound.columns}"

            df_custom_grouped_outbound = custom_group_buildings(df_outbound_melted, specific_groupings)
            df_custom_grouped_outbound['TO'] = df_custom_grouped_outbound['TO'].apply(lambda x: '  '.join(name_mapping.get(item, item) for item in x.split(' & ')))
            df_custom_grouped_outbound['TIME'] = df_custom_grouped_outbound['TIME'].apply(lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time() if isinstance(x, str) else x)
            df_custom_grouped_outbound['TIME'] = df_custom_grouped_outbound['TIME'].apply(lambda x: (datetime.datetime.combine(datetime.date.today(), x) + datetime.timedelta(minutes=14)).time())
            df_custom_grouped_outbound['TIME'] = df_custom_grouped_outbound['TIME'].apply(lambda x: x.strftime('%H:%M'))
            df_custom_grouped_outbound['DATE'] = tomorrows_date
            df_custom_grouped_outbound['FROM'] = 'EAC-C'
            df_final_outbound = df_custom_grouped_outbound[['DATE', 'NO OF UNITS', 'TIME', 'FROM', 'TO', 'CREW']].sort_values(by='TIME')
            outbound_output_path = os.path.join('downloads', 'cc_trip_rearranged_outbound.xlsx')
            df_final_outbound.to_excel(outbound_output_path, index=False)

            return render_template('download.html', inbound_path=inbound_output_path, outbound_path=outbound_output_path)

    return 'File upload failed'

@app.route('/download/<path:filename>', methods=['GET', 'POST'])
def download_file(filename):
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    os.makedirs('downloads', exist_ok=True)
    app.run(debug=True)
