import os
import csv
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import sqlite3
from tqdm import tqdm
import re


def get_images_sql(board, df_images):
    cols = list(df_images.columns)
    cols.remove('media_id')
    sql_cols = ', '.join(cols)
    sql_placeholders = ', '.join(['?'] * len(cols))
    sql_conflict = ', '.join([f'{col}=?' for col in cols])
    sql = f"""INSERT INTO `{board}_images` ({sql_cols}) VALUES ({sql_placeholders}) ON CONFLICT(`media_hash`) DO UPDATE SET {sql_conflict} RETURNING `media_id`;"""
    return sql


def get_d_images(board, df_images):
    d_images = {}
    for i, row_image in tqdm(df_images.iterrows(), desc=f'{board}_images dict'):
        d = row_image.to_dict()
        del d['media_id']
        d_images[row_image['media_id']] = list(d.values())
    return d_images


def get_board_params(board, df_board):
    params = []
    for i, row_board in tqdm(df_board.iterrows(), desc=f'{board} params'):
        row_board = row_board.to_dict()
        del row_board['doc_id']
        params.append(list(row_board.values()))
    return params


def get_d_threads(board, df_board, df_threads):
    params = []

    d_threads = {}
    for i, row_thread in tqdm(df_threads.iterrows(), desc=f'{board}_threads dict'):
        d_threads[row_thread['thread_num']] = list(row_thread.to_dict().values())

    for i, row_board in tqdm(df_board.iterrows(), desc=f'{board}_threads params'):
        row_thread_num = row_board['thread_num']
        if row_board['thread_num'] in d_threads:
            params.append(d_threads[row_thread_num])
            del d_threads[row_board['thread_num']]
    return params


def upsert_tables(cursor, board, df_board, df_images, df_threads):
    media_ids = []

    images_sql = get_images_sql(board, df_images)
    d_images = get_d_images(board, df_images)

    for i, row_board in tqdm(df_board.iterrows(), f'{board}_images'):
        row_media_id = row_board.to_dict()['media_id']
        media_id = None
        if row_media_id in d_images:
            media_id = do_upsert(cursor, images_sql, d_images[row_media_id], 'media_id')
            del d_images[row_media_id]

        media_ids.append(media_id)

    df_board['media_id'] = media_ids

    cols = list(df_board.columns)
    cols.remove('doc_id')
    params = get_board_params(board, df_board)
    do_upsert_many(cursor, board, cols, 'num', params)

    cols = list(df_threads.columns)
    params = get_d_threads(board, df_board, df_threads)
    do_upsert_many(cursor, f'{board}_threads', cols, 'thread_num', params)


def do_upsert(cursor, sql, values, returning):
    cursor.execute(sql, values + values)
    return cursor.fetchone()[returning]


def do_upsert_many(cursor: sqlite3.Cursor, table, cols, conflict_col, params):
    sql_cols = ', '.join(cols)
    sql_placeholders = ', '.join(['?'] * len(cols))
    sql_conflict = ', '.join([f'{col}=?' for col in cols])

    sql = f"""INSERT INTO `{table}` ({sql_cols}) VALUES ({sql_placeholders}) ON CONFLICT({conflict_col}) DO UPDATE SET {sql_conflict};"""

    cursor.executemany(sql, [val + val for val in params])


def make_path(*filepaths):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *filepaths)


def export_table_to_csv(conn, database, table_name, columns):
    csv_path = make_path('exports', database, f'{table_name}.csv')
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    with open(csv_path, mode='w', newline='', encoding='utf-8') as f:
        c = csv.writer(f, quoting=csv.QUOTE_ALL, escapechar='\\')
        c.writerow(columns)
        result = conn.execute(text(f"""select * from `{table_name}`;"""))
        counter = 1
        while True:
            print(f'{table_name} fetch #: {counter:<6,}')
            rows = result.fetchmany(50_000)
            if not rows:
                break
            for row in rows:
                try:
                    c.writerow(row)
                except:
                    print(row)
                    raise ValueError(row)
            counter += 1
        result.close()

    print(f"Table '{table_name}' exported to '{csv_path}'")


def mysql_to_csv():
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    table_skip_prefixes = [] # ['ck', 'g']

    with engine.connect() as conn:
        for table in tables:

            if any([table.startswith(prefix) for prefix in table_skip_prefixes]):
                continue

            columns = [column['name'] for column in inspector.get_columns(table)]
            export_table_to_csv(conn, DB_NAME, table, columns)


def dict_factory(cursor, row):
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def csv_to_sqlite():
    conn = sqlite3.connect(DB_SQLITE)
    conn.row_factory = dict_factory
    csv_paths = sorted([make_path('exports', DB_NAME, filename) for filename in os.listdir(make_path('exports', DB_NAME))])
    
    boards = set()
    for csv_path in csv_paths:
        board = os.path.basename(csv_path.split('_')[0].replace('.csv', ''))
        if re.match('index|idx|information|schema|seq|trig|proc|view|part', board, re.IGNORECASE):
            continue
        assert len(board) < 4
        assert len(board) > 0
        boards.add(board)
    boards = sorted(list(boards))

    cursor = conn.cursor()
    for board in boards:
        print(f'Reading tables for: {board}')

        df_board = pd.read_csv(make_path('exports', DB_NAME, f'{board}.csv'), low_memory=False)
        df_images = pd.read_csv(make_path('exports', DB_NAME, f'{board}_images.csv'), low_memory=False)
        df_threads = pd.read_csv(make_path('exports', DB_NAME, f'{board}_threads.csv'), low_memory=False)

        for df in [df_board, df_images, df_threads]:
            cols = df.select_dtypes('number')
            for col in cols:
                df[col] = df[col].astype('Int64')

        print(f'Pushing board: {board}')
        upsert_tables(cursor, board, df_board, df_images, df_threads)
        print(f'Done')
        conn.commit()
    
    cursor.close()
    conn.close()


if __name__=='__main__':
    DB_HOST = '192.168.1.201'
    DB_USER = 'user'
    DB_PASSWORD = 'password'
    DB_NAME = 'hayden'
    DB_PORT = 3306

    DB_SQLITE = '/meteor/ritual_bk.db'

    # Choose what you want to do

    # mysql_to_csv()
    # csv_to_sqlite()