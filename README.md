# Meteor

Asagi schema support only.

- Download MySQL database tables to CSVs
- Import CSVs into SQLite

## Install

`python3.12 -m pip install sqlalchemy tqdm`

## Use

- Configure the script in the bottom of `main.py`
- `python3.12 main.py`


## Benchmarks

Python 3.12.4 can insert 1.56 million 4chan posts, including board, images, and threads tables, into SQLite in ~18 seconds.

<pre>
Reading tables for: g
Pushing board: g
    286,728 it   [00:00, 415,962 it/s]
    1,562,466 it [00:13, 112,891 it/s]
    32,881 it    [00:00, 398,720 it/s]
    1,562,466 it [00:06, 253,350 it/s]
Done
</pre>