
import json
import os
import operator
import re
from dotenv import find_dotenv
from dotenv import dotenv_values



def load_jsonl_data(path):
    return [json.loads(x.strip()) for x in open(path)]


def cleanup(txt):
    txt = re.sub("\\n", "\n", txt)
    return re.sub("\'", "'", txt)


def bodify(txt):
    return f"<body>{txt}</body>"


def order_offsets(offsets):
    return sorted(offsets, key=operator.itemgetter(0))

def sectionify(txt, offsets):
    txt2 = ""
    start = 0
    for s, e, t in offsets:
        txt2 += txt[start:s]
        section = txt[s:e]
        div = f'<div id="{t}">{section}</div>'
        head = f"<h2>{div}</h2>"
        txt2 += head
        start = e
    txt2 += txt[start:-1]
    return txt2


def process(data):
    for d in data:
        txt = cleanup(d["data"])
        # txt = cleanup(txt)
        offsets = order_offsets(offsets)
        section = sectionify(txt, offsets)
        body = bodify(section)


def main():
    root_dir = os.path.dirname(find_dotenv())
    inital_data_path = os.path.join(root_dir, "data", "jdsections.jsonl")
    processed_data_path = os.path.join(root_dir, "data", "2.interim", "jdsections.jsonl")
    data = load_jsonl_data(inital_data_path)






    with conn:
        # create table
        print(f"creating table job_descriptions...")
        create_table(conn, sql_create_jd_table)
        print(f"populating job_descriptions...")
        populate_jd_table(data, conn)
        print("done")

 
if __name__ == '__main__':
    main()


