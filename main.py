import csv
import os
import re
import sqlite3
from typing import Optional

from pydantic import BaseModel, field_validator
from sqlalchemy import create_engine, inspect, text
from tqdm import tqdm


class ImageRow(BaseModel):
    media_id: int
    media_hash: str
    media: str
    preview_op: str
    preview_reply: str
    total: int
    banned: int

class BoardRow(BaseModel):
    doc_id: int
    media_id: Optional[int]
    poster_ip: str
    num: int
    subnum: int
    thread_num: int
    op: int
    timestamp: int
    timestamp_expired: int
    preview_orig: str
    preview_w: int
    preview_h: int
    media_filename: str
    media_w: int
    media_h: int
    media_size: int
    media_hash: str
    media_orig: str
    spoiler: int
    deleted: int
    capcode: str
    email: str
    name: str
    trip: str
    title: str
    comment: str
    delpass: str
    sticky: int
    locked: int
    poster_hash: str
    poster_country: str
    exif: str

class ThreadRow(BaseModel):
    thread_num: int
    time_op: int
    time_last: int
    time_bump: Optional[int]
    time_ghost: Optional[int]
    time_ghost_bump: Optional[int]
    time_last_modified: Optional[int]
    nreplies: int
    nimages: int
    sticky: int
    locked: int

    @field_validator('time_ghost', 'time_ghost_bump', mode='before')
    @classmethod
    def empty_str_to_none(cls, v):
        if v == '':
            return None
        return v


def get_images_sql(board):
    cols = list(ImageRow.model_fields)
    cols.remove('media_id')
    sql_cols = ', '.join(cols)
    sql_placeholders = ', '.join(['?'] * len(cols))
    sql_conflict = ', '.join([f'{col}=?' for col in cols])
    sql = f"""INSERT INTO `{board}_images` ({sql_cols}) VALUES ({sql_placeholders}) ON CONFLICT(`media_hash`) DO UPDATE SET {sql_conflict} RETURNING `media_id`;"""
    return sql


def get_media_id_to_media_row(csv_path_images):
    d = dict()
    with open(csv_path_images, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader):
            d[row['media_id']] = [row[k] for k in row if k != 'media_id']
    return d


def get_thread_num_to_thread_row(csv_path_threads):
    thread_num_to_thread_row = dict()
    with open(csv_path_threads, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader):
            thread_num_to_thread_row[row['thread_num']] = [row[k] for k in row]
    return thread_num_to_thread_row


def upsert_tables(cursor, board, csv_path_board, csv_path_images, csv_path_threads):
    board_params = []
    thread_params = []

    images_sql = get_images_sql(board)
    media_id_to_media_row = get_media_id_to_media_row(csv_path_images)
    thread_num_to_thread_row = get_thread_num_to_thread_row(csv_path_threads)

    with open(csv_path_board, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader):
            media_id = None
            if row['media_id'] in media_id_to_media_row:
                media_id = do_upsert(cursor, images_sql, media_id_to_media_row[row['media_id']], 'media_id')

            row['media_id'] = media_id
            board_params.append([row[k] for k in row if k != 'doc_id'])

            if row['thread_num'] in thread_num_to_thread_row:
                thread_params.append(thread_num_to_thread_row[row['thread_num']])

    board_cols = list(BoardRow.model_fields)
    board_cols.remove('doc_id')
    do_upsert_many(cursor, board, board_cols, 'num', board_params)

    thread_cols = list(ThreadRow.model_fields)
    do_upsert_many(cursor, f'{board}_threads', thread_cols, 'thread_num', thread_params)


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


def export_table_to_csv(conn, csv_path, table_name, columns):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    with open(csv_path, mode='w', encoding='utf-8') as f:
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


def mysql_to_csv(boards_to_skip):
    """Skips tables that have already been exported."""
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    with engine.connect() as conn:
        for table in tables:

            csv_path = make_path('exports', DB_NAME, f'{table}.csv')
            if os.path.isfile(csv_path):
                print(f'{csv_path} exists, skipping {table} export.')
                continue

            if boards_to_skip and any([table.startswith(board) for board in boards_to_skip]):
                continue

            columns = [column['name'] for column in inspector.get_columns(table)]
            export_table_to_csv(conn, csv_path, table, columns)


def dict_factory(cursor, row):
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def csv_to_sqlite(boards_to_import):
    conn = sqlite3.connect(DB_SQLITE)
    conn.row_factory = dict_factory
    csv_paths = sorted([make_path('exports', DB_NAME, filename) for filename in os.listdir(make_path('exports', DB_NAME))])
    
    boards = set()
    for csv_path in csv_paths:
        board = os.path.basename(csv_path.split('_')[0].replace('.csv', ''))
        if board not in boards_to_import:
            continue
        if re.match('index|idx|information|schema|seq|trig|proc|view|part', board, re.IGNORECASE):
            continue
        assert len(board) < 4
        assert len(board) > 0
        boards.add(board)
    boards = sorted(list(boards))

    cursor = conn.cursor()
    for board in boards:
        print(f'Reading tables for: {board}')

        csv_path_board = make_path('exports', DB_NAME, f'{board}.csv')
        csv_path_images = make_path('exports', DB_NAME, f'{board}_images.csv')
        csv_path_threads = make_path('exports', DB_NAME, f'{board}_threads.csv')

        print(f'Pushing board: {board}')
        upsert_tables(cursor, board, csv_path_board, csv_path_images, csv_path_threads)
        print(f'Done')
        conn.commit()
    
    cursor.close()
    conn.close()


def create_non_existing_tables(boards):
    """Only created tables if they don't already exist."""
    conn = sqlite3.connect(DB_SQLITE)
    cursor = conn.cursor()
    for board in boards:
        try:
            sql = f'SELECT * FROM {board} LIMIT 1;'
            conn.execute(sql)
            print(f'{board} tables already exist.')
        except Exception:
            print(f'Creating tables for {board}.')
            with open(make_path('schema.sql')) as f:
                sql = f.read()
            sqls = sql.replace('%%BOARD%%', board).split(';')
            for sql in sqls:
                conn.execute(sql)
            conn.commit()
    cursor.close()
    conn.close()


if __name__=='__main__':
    DB_HOST = '192.168.1.201'
    DB_USER = ''
    DB_PASSWORD = ''
    DB_NAME = 'hayden'
    DB_PORT = 3306

    DB_SQLITE = '/home/ritual1.db'



    ########## Choose what you want to do ###########

    # boards_to_create = ['ck', 'g', 'mu', 't', 'r9k']
    # create_non_existing_tables(boards_to_create)

    # boards_to_not_export = [] # ['mu']
    # mysql_to_csv(boards_to_not_export)

    boards_to_import = ['g']
    csv_to_sqlite(boards_to_import)
