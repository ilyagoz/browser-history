import os, binascii, array, urllib.parse, argparse, sys
import sqlite3
from tqdm import tqdm

golden_ratio = 0x9E3779B9
max_int = 2**32 - 1

def GenerateGUID():
	return binascii.b2a_base64(os.urandom(9), newline=False)

def rotate_left_5(value):
    return ((value << 5) | (value >> 27)) & max_int

def add_to_hash(hash_value, value):
    return (golden_ratio * (rotate_left_5(hash_value) ^ value))  & max_int

def hash_simple(url):
    hash_value = 0
    for char in url.encode('utf-8'):
        hash_value = add_to_hash(hash_value, char)
    return hash_value

def url_hash(url):
    prefix, _ = url.split(':', 1)
    return ((hash_simple(prefix) & 0x0000FFFF) << 32) + hash_simple(url)

def progress_handler():
    print('Still working...')
    return

def populate_moz_places(conn_src, conn_tgt):
    conn_src.set_progress_handler(progress_handler, 3000000)
    cursor_tgt = conn_tgt.cursor()
    cursor_src = conn_src.cursor()

    print("Fetching data from source moz_places and moz_origins...")

    cursor_src.execute("""SELECT 
    moz_places.*, 
    moz_origins.id AS moz_origins_id, 
    moz_origins.prefix AS moz_origins_prefix, 
    moz_origins.host AS moz_origins_host, 
    moz_origins.frecency AS moz_origins_frecency 
    FROM moz_places 
    LEFT JOIN moz_origins ON moz_places.origin_id = moz_origins.id
    ;""")
    
    rows = cursor_src.fetchall()

    print(len(rows), "rows fetched, processing...")

    for place in tqdm(rows):
        if place["moz_origins_prefix"] is None or place["moz_origins_host"] is None:
            origin_id_tgt = 0
        else:
            cursor_tgt.execute("""
                SELECT EXISTS(SELECT id FROM moz_origins WHERE prefix=? AND host=?) AS ex
                ;""",
                (place["moz_origins_prefix"], place["moz_origins_host"],))
            exists = cursor_tgt.fetchone()

            if exists[0] != 0:
                cursor_tgt.execute("""
                SELECT id FROM moz_origins WHERE prefix=? AND host=?
                ;""",
                (place["moz_origins_prefix"], place["moz_origins_host"],))
                origin_id_tgt = cursor_tgt.fetchone()[0]
            else:
                cursor_tgt.execute("""
            	    INSERT INTO moz_origins (prefix, host, frecency)
                    VALUES (?, ?, ?)
                    ;""",
                (place["moz_origins_prefix"], place["moz_origins_host"], place["moz_origins_frecency"],))
                origin_id_tgt = cursor_tgt.lastrowid
    
        # Теперь у нас есть origin_id, соответствующий целевой таблице moz_origins, или == 0.

        cursor_tgt.execute("""
                SELECT EXISTS(SELECT id FROM moz_places WHERE url_hash=?) AS ex
                ;""",
                (place["url_hash"],))
        exists = cursor_tgt.fetchone()
        if exists[0] != 0:
            # Предположим, что место в таблице задано верно, и делать ничего не надо.
            continue
        else:
            cursor_tgt.execute("""
                INSERT INTO moz_places (url, title, rev_host, visit_count, hidden, typed, frecency, last_visit_date, guid, foreign_count, url_hash, 
                description, preview_image_url, origin_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ;""",
            (place["url"], place["title"], place["rev_host"], place["visit_count"], place["hidden"],
	        place["typed"], place["frecency"], place["last_visit_date"], place["guid"], place["foreign_count"],
	        place["url_hash"], place["description"], place["preview_image_url"], 
	        origin_id_tgt,))
    cursor_src.close()
    cursor_tgt.close()
    conn_src.set_progress_handler(None, 0)

def populate_moz_historyvisits(conn_src, conn_tgt):
    # Теперь копируем собственно историю, то есть moz_historyvisits
    cursor_tgt = conn_tgt.cursor()
    cursor_src = conn_src.cursor()
    print('Fetching data from moz_historyvisits...')
    conn_src.set_progress_handler(progress_handler, 3000000)
    cursor_src.execute("""SELECT 
    * 
    FROM moz_historyvisits
    ;""")

    rows = cursor_src.fetchall()
    print(len(rows), "rows fetched, processing...")

    conn_src.set_progress_handler(None, 0)

    # В колонке from_visit в moz_historyvisits указаны id визитов в этой же 
    # таблице, а не в moz_places. Причем визит from_visit может быть уже давно стерт.
    # А к моменту вставки текущего визита его from_visit может быть еще не вставлен.
    # Поэтому придется сделать таблицу соответствия старых id визитов новым. После вставки всех
    # визитов придется пройтись по колонке from_visit и заменить старые id новыми или нулями,
    # если новых нет.
    ids_from_visit = dict()
    added_ids = array.array('I')

    for visit in tqdm(rows):
        # Найдем place_id в целевой таблице moz_places, соответствующий place_id в исходной таблице.
        # У визитов в целевой и исходной таблице могут быть разные id.
    
        cursor_src.execute("""
            SELECT url_hash FROM moz_places WHERE id=?
            ;""",
            (visit["place_id"],))
        hash_place = cursor_src.fetchone()[0]

        cursor_tgt.execute("""
            SELECT id FROM moz_places WHERE url_hash=?
            ;""",
            (hash_place,))
        # Мы уже заполнили таблицу moz_places, поэтому не найти там соответствующий
        # визиту url можно, только если его не было в исходной таблице, а значит,
        # база была в несогласованном состоянии.
        place_id = cursor_tgt.fetchone()[0]    

        # Проверим, не записан ли уже этот визит в целевой таблице moz_historyvisits
        cursor_tgt.execute("""
            SELECT id FROM moz_historyvisits WHERE place_id=? AND visit_date=?
        ;""",
        (place_id, visit["visit_date"],))
        visit_id = cursor_tgt.fetchone()

        if visit_id is None: # Визит не записан, запишем его
            cursor_tgt.execute("""
            INSERT INTO moz_historyvisits (from_visit, place_id, visit_date, visit_type, session)
            VALUES (?, ?, ?, ?, ?)
            ;""",
            (visit["from_visit"], 
            place_id,
            visit["visit_date"], visit["visit_type"], visit["session"],))
            newvisit_id = cursor_tgt.lastrowid
            added_ids.append(newvisit_id)
        else:
            newvisit_id = visit_id[0]
        
      
        ids_from_visit[visit["id"]] = newvisit_id

    # Проход для заполнения from_visit. все будет неверно, если мы уже импортировали
    # другие истории и там висят заходы с from_visit, которых в этой партии не было.
    # Поэтому надо обрабатывать только визиты с id, которые были добавлены здесь.
    print('Updating from_visit...')
    for i in tqdm(added_ids):
        cursor_tgt.execute("""
        SELECT from_visit FROM moz_historyvisits WHERE id=?
        ;""",
        (i,))
        visit_id = cursor_tgt.fetchone()[0]
        if visit_id in ids_from_visit:
            new_from_visit = ids_from_visit[visit_id]
        else:
            new_from_visit = 0
        cursor_tgt.execute("""
        UPDATE moz_historyvisits SET from_visit=? WHERE id=?
        ;""",
        (new_from_visit, i,))


def merge_mozilla(to_places, from_places):
    conn_tgt = sqlite3.connect(to_places)
    conn_tgt.text_factory = lambda b: b.decode(errors = 'ignore')

    conn_src = sqlite3.connect(from_places)
    conn_src.text_factory = lambda b: b.decode(errors = 'ignore')

    conn_tgt.create_function("GENERATE_GUID", 0, GenerateGUID)
    conn_tgt.create_function("hash", 1, url_hash)
    conn_tgt.row_factory = sqlite3.Row
    conn_src.create_function("GENERATE_GUID", 0, GenerateGUID)
    conn_src.create_function("hash", 1, url_hash)
    conn_src.row_factory = sqlite3.Row

    try:
        populate_moz_places(conn_src, conn_tgt)
    except:
        print('Error populating moz_places, not saving data.')
        conn_tgt.close()
        conn_src.close()
        return
    conn_tgt.commit()

    try:
        populate_moz_historyvisits(conn_src, conn_tgt)
    except:
        print('Error populating moz_historyvisits, not saving data.')
        conn_tgt.close()
        conn_src.close()
        return
    conn_tgt.commit()

    conn_tgt.close()
    conn_src.close()

def merge_chrome(to_places, from_places):
    conn_chr = sqlite3.connect(from_places)
    conn_chr.text_factory = lambda b: b.decode(errors = 'ignore')
    conn_chr.row_factory = sqlite3.Row

    conn_moz = sqlite3.connect(to_places)
    conn_moz.text_factory = lambda b: b.decode(errors = 'ignore')
    conn_moz.row_factory = sqlite3.Row
    conn_moz.create_function("GENERATE_GUID", 0, GenerateGUID)
    conn_moz.create_function("hash", 1, url_hash)

    cursor_chr = conn_chr.cursor()
    cursor_moz = conn_moz.cursor()

    print('Fetching data from Chrome urls...')
    conn_chr.set_progress_handler(progress_handler, 3000000)
    cursor_chr.execute("""
    SELECT visits.id, visits.url as url_id, visit_time, from_visit, transition, 
    urls.url, title, visit_count, typed_count, last_visit_time, hidden
    FROM visits LEFT JOIN urls ON urls.id = visits.url ORDER BY visit_time
    ;""")

    rows = cursor_chr.fetchall()
    print(len(rows), "rows fetched, processing...")

    added_places =0
    modified_places =0
    added_visits =0

    ids_from_visit = dict()
    added_ids = array.array('I')

    for visit in tqdm(rows[:]):
        # Вставить префикс и хост данного места в moz_origins, если его там еще нет
        # и получить новый id.
        split_url = urllib.parse.urlsplit(visit["url"])
        prefix = split_url.scheme + "://"
        cursor_moz.execute("""
        SELECT id FROM moz_origins WHERE prefix=? AND host=?
        ;""",
        (prefix, split_url.hostname,))
        moz_origin = cursor_moz.fetchall()

        if len(moz_origin) == 0:
            cursor_moz.execute("""
            INSERT OR IGNORE INTO moz_origins (prefix, host, frecency)
            VALUES (?, ?, ?)
            ;""",
            (prefix, "" if split_url.hostname is None else split_url.hostname, -1,))
            origin_id = cursor_moz.lastrowid
        else:
            origin_id = moz_origin[0][0]

        # Вставить место данного визита в moz_places, если там его еще нет
        # и получить новый id.
        hash=url_hash(visit["url"])
        cursor_moz.execute("""
        SELECT id,last_visit_date, visit_count FROM moz_places WHERE url_hash=?
        ;""", (hash,))
    
        moz_place = cursor_moz.fetchall()
        len_places = len(moz_place)
        # В moz_places все url занесены только один раз или ни одного.
        assert (len_places == 1) or (len_places == 0)

        # В Chrome время сдвинуто относительно Mozilla.
        last_visit_date = visit["last_visit_time"] - 11644473600000000
       
        if len_places == 0:
            # См. также https://stackoverflow.com/questions/931092/how-do-i-reverse-a-string-in-python
            rev_host = "" if split_url.hostname is None else split_url.hostname[::-1] + '.'
            cursor_moz.execute("""
            INSERT INTO moz_places (url, title, rev_host, visit_count, hidden, typed, frecency, 
            last_visit_date, guid, foreign_count, url_hash, 
            description, preview_image_url, origin_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ;""",
            (visit["url"], visit["title"], rev_host, visit["visit_count"], visit["hidden"], visit["typed_count"], -1, 
            last_visit_date, GenerateGUID(), 0, hash, 
            None, "", origin_id,))
            place_id = cursor_moz.lastrowid
            added_places += 1
        else:
            place_id = moz_place[0]["id"]
            if last_visit_date > (0 if moz_place[0]["last_visit_date"] is None else moz_place[0]["last_visit_date"]):
                cursor_moz.execute("""
                UPDATE moz_places SET last_visit_date=?, visit_count=? WHERE id=?
                ;""",
                (last_visit_date, visit["visit_count"], place_id,))
                modified_places += 1

        # Вставить данный визит в moz_historyvisits, если там его еще нет
        # и получить новый id.
        cursor_moz.execute("""
            SELECT id FROM moz_historyvisits WHERE place_id=? AND visit_date=?
            ;""",
            (place_id, visit["visit_time"],))
        visit_id = cursor_moz.fetchall()
        len_visits = len(visit_id)
        assert(len_visits==0 or len_visits==1)

        # Добавить визит, обновить таблицу соответствия id для from_visit
        visit_date = visit["visit_time"] - 11644473600000000
        if len_visits == 0:
            cursor_moz.execute("""
            INSERT INTO moz_historyvisits (from_visit, place_id, visit_date)
            VALUES (?, ?, ?)
            ;""",
            (visit["from_visit"], 
            place_id,
            visit_date,))
            newvisit_id = cursor_moz.lastrowid
            added_ids.append(newvisit_id)
        else:
            newvisit_id = visit_id[0][0]
    
        ids_from_visit[visit["id"]] = newvisit_id

    # Проход для заполнения from_visit. все будет неверно, если мы уже импортировали
    # другие истории и там висят заходы с from_visit, которых в этой партии не было.
    # Поэтому надо обрабатывать только визиты с id, которые были добавлены здесь.
    print('Updating from_visit...')
    for i in tqdm(added_ids):
        cursor_moz.execute("""
        SELECT from_visit FROM moz_historyvisits WHERE id=?
        ;""",
        (i,))
        visit_id = cursor_moz.fetchone()[0]
        if visit_id in ids_from_visit:
            new_from_visit = ids_from_visit[visit_id]
        else:
            new_from_visit = 0
        cursor_moz.execute("""
        UPDATE moz_historyvisits SET from_visit=? WHERE id=?
        ;""",
        (new_from_visit, i,))
    print(added_places, "places added,", modified_places, "modified") 
    conn_moz.commit()

    conn_chr.close()
    conn_moz.close()
    return

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Merge browser history databases.")
    argparser.add_argument("--from_places", help="Source history database.",  required=True)
    argparser.add_argument("--to_places", help="Destination history database.", required=True)
    args=argparser.parse_args()

    test_db = sqlite3.connect(args.from_places)
    res = test_db.execute("SELECT EXISTS(SELECT name FROM sqlite_master WHERE type='table' AND name='moz_places');")
    if (res.fetchall()[0] != 1):
        print(args.from_places, 'does not seem to be Mozilla browser history database.')
        test_db.close()
        sys.exit(1)
    test_db.close()
    test_db = sqlite3.connect(args.to_places)
    res = test_db.execute("SELECT EXISTS(SELECT name FROM sqlite_master WHERE type='table' AND name='moz_historyvisits');")
    if (res.fetchall()[0] != 1):
        print(args.to_places, 'does not seem to be Mozilla browser history database.')
        test_db.close()
        sys.exit(1)
    test_db.close()
    
    if not os.access(args.from_places, os.R_OK):
        print(args.from_places, "is not readable")
        sys.exit(1)
    if not os.access(args.to_places, os.W_OK):
        print(args.to_places, "is not writable")
        sys.exit(1)
    
    merge_mozilla(args.to_places, args.from_places)
    #merge_chrome(args.to_places, args.from_places)

