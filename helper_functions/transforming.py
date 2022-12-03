import pandas as pd

def column_processor(df):
    df = df.str.lower()
    df = df.str.replace('-', ' ')
    df = df.str.strip()
    return df

def transformation(university):
    df = pd.read_csv(f"./files/{university}.csv", index_col=0)

    df['last_name'] = df['first_name'].str.split('-', expand = True)[1]
    df['first_name'] = df['first_name'].str.split('-', expand = True)[0]

    columns_to_transform = ['university', 'career', 'first_name', 'last_name']
    for column in columns_to_transform:
        df[column] = column_processor(df[column])

    df.to_csv(f'./datasets/{university}_transformed.csv')
    