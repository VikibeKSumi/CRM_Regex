import pandas as pd
from fastapi import FastAPI

app = FastAPI

# Creating two messy lists that need to be merged
raw_data = {
    'Customer_Name': [
        'John Smith', 'jane doe', 'Mr. Bob Vance', 'Smith, John',
        'J. Doe', 'Bob Vance (Refrigeration)', 'Alice Wonder', 'alice wonder'
    ],
    'Phone_Number': [
        '(555) 123-4567', '555.123.4567', '+1 555 123 4567', '5551234567',
        'NaN', '555-987-6543', '(555) 999-8888', '555.999.8888'
    ],
    'Email': [
        'john@gmail.com', 'jane.doe@work.org', 'bob@vanceref.com', 'JOHN.SMITH@GMAIL.COM',
        'j.doe@work.org', 'bob@vanceref.com', 'alice@wonder.land', 'alice@wonder.land'
    ]
}


def start_pipeline(df):
  return df.copy()

def clean_column_names(df):
  df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
  return df

def name_fix(df):
  if 'customer_name' in df.columns:
    # Function to reformat 'Last, First' to 'First Last'
    def reformat_name_order(name):
      if isinstance(name, str) and ',' in name:
          parts = [p.strip() for p in name.split(',')]
          if len(parts) == 2:
              return f"{parts[1]} {parts[0]}"
      return name

    # Apply the reformat_name_order to handle 'Last, First' cases
    df['customer_name'] = df['customer_name'].apply(reformat_name_order)

    df['customer_name'] = (
    df['customer_name']
      .str.replace(r'[^a-zA-Z\s]', '', regex=True) # Remove non-alphabetic and non-whitespace characters
      .str.replace(r'Refrigeration', '', regex=True) # Remove the word 'Refrigeration'
      .str.replace(r'^(Mr|Mrs|Ms|Dr)\s+', '', regex=True) # Remove honorifics
      .str.title() # Convert to Title Case
    )

    #Split columns for both first and last name
    split_names = df['customer_name'].str.split(' ', n=1, expand=True)
    df['first_name'] = split_names[0]
    df['last_name'] = split_names[1].fillna('')

    # Remove the original customer_name column if you only want First_Name and Last_Name
    df = df.drop(columns=['customer_name'])
  return df # Ensure df is always returned

def clean_phone(df):
  if 'phone_number' in df.columns: # Changed to 'phone_number' (lowercase)
    # Remove all non-numeric characters
    df['phone_number'] = df['phone_number'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    # Format as XXX-XXX-XXXX
    df['phone_number'] = df['phone_number'].apply(
        lambda x: f'{x[0:3]}-{x[3:6]}-{x[6:10]}' if len(x) == 10
        else (f'{x[1:4]}-{x[4:7]}-{x[7:]}' if len(x) == 11 and x.startswith('1')
        else x)
    )
  return df

def remove_duplicates(df):
  df = df.drop_duplicates(subset=['first_name','last_name'])
  return df

df_raw = pd.DataFrame(raw_data)

cleaned_df = (
    df_raw
    .pipe(start_pipeline)
    .pipe(clean_column_names)
    .pipe(name_fix)
    .pipe(clean_phone)
    .pipe(remove_duplicates)
)

@get("/")
def read_root():
    return cleaned_df
