# Meteor

Asagi schema support only.

- Download MySQL database tables to CSVs
- Import CSVs into SQLite

## Install

`python3 -m pip install sqlalchemy pandas tqdm`

## Use

- Configure the script in the bottom of `main.py`
- `python3 main.py`


## Benchmarks

Python 3.12.4 can insert 1.56 million 4chan posts, including board, images, threads tables, into SQLite in ~3 minutes.

<pre>
Reading tables for: ck
Pushing board: ck
ck_images dict:     92,229it [00:03, 29,208it/s]
ck_images:         566,078it [00:25, 22,419it/s]
ck params:         566,078it [00:23, 24,353it/s]
ck_threads dict:    19,908it [00:00, 26,553it/s]
ck_threads params: 566,078it [00:14, 40,025it/s]
Done

Reading tables for: g
Pushing board: g
g_images dict:      286,728it [00:09, 28,721it/s]
g_images:         1,562,466it [01:07, 23,092it/s]
g params:         1,562,466it [01:04, 24,391it/s]
g_threads dict:      3,2881it [00:01, 28,477it/s]
g_threads params: 1,562,466it [00:37, 41,773it/s]
Done
</pre>

Note: Python 3.12 offers a ~10-20% speed boost over Python 3.10.