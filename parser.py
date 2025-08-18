import pandas as pd
import gzip
import json

# Define a function to parse the JSON string and extract NIC5DigitId and Description
def parse_activities(activity_json_str):
    try:
        activities = json.loads(activity_json_str)
        # Extract NIC5DigitId and Description from the first item in the list
        if isinstance(activities, list) and len(activities) > 0:
            return activities[0]['NIC5DigitId'], activities[0]['Description']
        else:
            return None, None
    except Exception:
        return None, None

district_name = str(input("Enter your district name:    "))
district_name = district_name.lower()
with gzip.open(fr'C:\Users\prithvirajp\Desktop\MySql\week5\complete_district_data\{district_name}_complete_data.json.gz', 'rt', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data['data'])


# Apply the parsing function to the 'Activities' column
parsed_activities = df['Activities'].apply(parse_activities)

# Create new columns in the dataframe
df['NIC5Digit_code'] = parsed_activities.apply(lambda x: x[0])
df['Activity_Description'] = parsed_activities.apply(lambda x: x[1])


del df['Activities']
print(df)
df.to_csv('district_data.csv',index=False)